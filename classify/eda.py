"""Small EDA utilities for events.

Usage: run this file to build a log-histogram of event volumes across all
`events/events_*.json` files and save the plot as `outputs/log_volume_hist.png`.

This script extracts the top-level `volume` field from every event. It uses
log10(volume + 1) so bins can include zero-volume markets (log undefined for
zero otherwise).
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Iterable, List

import matplotlib.pyplot as plt
import numpy as np


def iter_event_files(events_dir: Path) -> Iterable[Path]:
	for p in sorted(events_dir.glob("events_*.json")):
		yield p


def load_volumes(events_dir: Path) -> List[float]:
	"""Load volume from each event across files in events_dir.

	Returns a list of floats representing top-level `volume` values.
	"""
	volumes: List[float] = []
	for path in iter_event_files(events_dir):
		with path.open("r", encoding="utf8") as fh:
			try:
				events = json.load(fh)
			except json.JSONDecodeError:
				# skip malformed files
				continue

		if not isinstance(events, list):
			continue

		for ev in events:
			# top-level volume field (float) is preferred
			v = ev.get("volume") if isinstance(ev, dict) else None
			if v is None:
				# some nested markets also contain strings, fall back on markets[0]
				try:
					markets = ev.get("markets", [])
					if markets and isinstance(markets, list):
						mv = markets[0].get("volumeNum") or markets[0].get("volume")
						v = float(mv) if mv is not None else None
				except Exception:
					v = None

			# coerce to float if possible
			try:
				if v is not None:
					volumes.append(float(v))
			except Exception:
				# ignore anything that can't be coerced
				continue

	return volumes


def load_volumes_by_category(events_dir: Path) -> dict:
	"""Return a mapping category -> list[float] for event volumes.

	Categories that are missing or falsy are grouped under the string "(unknown)".
	"""
	out: dict = {}
	for path in iter_event_files(events_dir):
		with path.open("r", encoding="utf8") as fh:
			try:
				events = json.load(fh)
			except json.JSONDecodeError:
				continue

		if not isinstance(events, list):
			continue

		for ev in events:
			if not isinstance(ev, dict):
				continue

			cat = ev.get("category") or "(unknown)"
			v = ev.get("volume")
			if v is None:
				markets = ev.get("markets", [])
				if markets and isinstance(markets, list):
					mv = markets[0].get("volumeNum") or markets[0].get("volume")
					v = float(mv) if mv is not None else None

			try:
				v = float(v) if v is not None else None
			except Exception:
				v = None

			if v is None:
				# skip missing or unparsable
				continue

			out.setdefault(cat, []).append(v)

	return out


def plot_log_hist(volumes: Iterable[float], out_path: Path, bins: int = 60) -> None:
	arr = np.array(list(volumes), dtype=float)
	if arr.size == 0:
		raise RuntimeError("no volumes to plot")

	# log10(volume + 1) handles zeros gracefully and keeps units interpretable
	logv = np.log10(arr + 1.0)

	plt.figure(figsize=(9, 6))
	plt.hist(logv, bins=bins, color="#2c7fb8", edgecolor="#08306b")
	plt.xlabel("log10(volume + 1)")
	plt.ylabel("count")
	plt.title("Log10(volume + 1) histogram across all events")
	plt.grid(alpha=0.25)
	out_path.parent.mkdir(parents=True, exist_ok=True)
	plt.tight_layout()
	plt.savefig(out_path, dpi=150)
	plt.close()


def plot_log_hist_by_category(volumes_by_cat: dict, out_path: Path, max_categories: int = 12, bins: int = 50) -> None:
	"""Create a vertically stacked set of log10(volume+1) histograms by category.

	- volumes_by_cat: mapping category->list[float]
	- max_categories: maximum number of categories to show (largest categories by count). Others will be aggregated into "Other".
	"""
	# pick categories by size
	items = sorted(volumes_by_cat.items(), key=lambda kv: len(kv[1]), reverse=True)
	if len(items) == 0:
		raise RuntimeError("no categories to plot")

	if len(items) > max_categories:
		shown = items[: max_categories - 1]
		others = items[max_categories - 1 :]
		other_vols = []
		for _, v in others:
			other_vols.extend(v)
		shown.append(("Other", other_vols))
	else:
		shown = items

	# compute log values for all shown categories
	log_values = [np.log10(np.array(v, dtype=float) + 1.0) for _, v in shown]

	# determine common x limits
	all_logs = np.concatenate(log_values) if len(log_values) > 0 else np.array([])
	xmin, xmax = float(np.nanmin(all_logs)), float(np.nanmax(all_logs))

	n = len(shown)
	fig, axes = plt.subplots(nrows=n, ncols=1, figsize=(9, max(2.2 * n, 6)), sharex=True)
	if n == 1:
		axes = [axes]

	for ax, (cat, vv), logs in zip(axes, shown, log_values):
		ax.hist(logs, bins=bins, color="#2c7fb8", edgecolor="#08306b")
		ax.set_ylabel(cat)
		ax.grid(alpha=0.2)
		ax.set_xlim(xmin - 0.1, xmax + 0.1)

	axes[-1].set_xlabel("log10(volume + 1)")
	fig.suptitle("Log10(volume + 1) by category (stacked)")
	fig.tight_layout(rect=[0, 0, 1, 0.96])
	out_path.parent.mkdir(parents=True, exist_ok=True)
	fig.savefig(out_path, dpi=150)
	plt.close(fig)


def load_yes_prices_by_category(events_dir: Path) -> dict:
	"""Extract 'Yes' outcome prices for markets grouped by event category.

	Returns a mapping category -> list[float] with prices in the range [0, 1].
	"""
	out: dict = {}
	for path in iter_event_files(events_dir):
		with path.open("r", encoding="utf8") as fh:
			try:
				events = json.load(fh)
			except json.JSONDecodeError:
				continue

		if not isinstance(events, list):
			continue

		for ev in events:
			if not isinstance(ev, dict):
				continue

			cat = ev.get("category") or "(unknown)"

			markets = ev.get("markets", [])
			if not markets or not isinstance(markets, list):
				continue

			for m in markets:
				if not isinstance(m, dict):
					continue

				outcomes = m.get("outcomes")
				prices = m.get("outcomePrices")

				# outcomes & prices may be JSON strings or lists
				try:
					if isinstance(outcomes, str):
						outcomes = json.loads(outcomes)
				except Exception:
					outcomes = None

				try:
					if isinstance(prices, str):
						prices = json.loads(prices)
				except Exception:
					prices = None

				if not outcomes or not prices or not isinstance(outcomes, (list, tuple)) or not isinstance(prices, (list, tuple)):
					continue

				# find 'Yes' case-insensitively
				yes_indices = [i for i, o in enumerate(outcomes) if isinstance(o, str) and o.strip().lower() == "yes"]
				for i in yes_indices:
					try:
						p = float(prices[i])
					except Exception:
						continue
					# Only accept prices inside the [0,1] probability range
					if p < 0 or p > 1:
						continue
					out.setdefault(cat, []).append(p)

	return out


def plot_yes_price_by_category(yes_by_cat: dict, out_path: Path, max_categories: int = 16) -> None:
	"""Create a boxplot (with jittered points) of Yes outcome prices per category.

	Shows top categories by sample count; remaining categories are grouped into 'Other'.
	"""
	items = sorted(yes_by_cat.items(), key=lambda kv: len(kv[1]), reverse=True)
	if not items:
		raise RuntimeError("no yes-prices to plot")

	if len(items) > max_categories:
		shown = items[: max_categories - 1]
		others = items[max_categories - 1 :]
		other_vals = []
		for _, vals in others:
			other_vals.extend(vals)
		shown.append(("Other", other_vals))
	else:
		shown = items

	labels = [k for k, _ in shown]
	data = [v for _, v in shown]

	# create figure
	n = len(data)
	fig, ax = plt.subplots(figsize=(max(8, n * 0.6), 6))

	# boxplot
	b = ax.boxplot(data, labels=labels, patch_artist=True, showfliers=False)
	for patch in b['boxes']:
		patch.set_facecolor('#c6dbef')
		patch.set_edgecolor('#08306b')

	# jittered points
	for i, vals in enumerate(data, start=1):
		xs = np.random.normal(i, 0.08, size=len(vals))
		ax.scatter(xs, vals, alpha=0.35, s=9, color='#2c7fb8')

	ax.set_ylabel('Yes outcome price')
	ax.set_ylim(-0.02, 1.02)
	plt.xticks(rotation=45, ha='right')
	ax.set_title('Distribution of Yes outcome prices by category')
	plt.tight_layout()
	out_path.parent.mkdir(parents=True, exist_ok=True)
	fig.savefig(out_path, dpi=150)
	plt.close(fig)


def print_summary(volumes: Iterable[float]) -> None:
	arr = np.array(list(volumes), dtype=float)
	print("count:", arr.size)
	print("min:", float(np.nanmin(arr)) if arr.size else "n/a")
	print("median:", float(np.nanmedian(arr)) if arr.size else "n/a")
	print("mean:", float(np.nanmean(arr)) if arr.size else "n/a")
	print("max:", float(np.nanmax(arr)) if arr.size else "n/a")


def main() -> None:
	repo_root = Path(__file__).resolve().parents[1]
	events_dir = repo_root / "events"
	out_path = repo_root / "outputs" / "log_volume_hist.png"

	volumes = load_volumes(events_dir)
	if len(volumes) == 0:
		print("No volumes found in", events_dir)
		return

	print_summary(volumes)
	plot_log_hist(volumes, out_path)
	print(f"Saved histogram to {out_path}")

	# per-category stacked figure
	volumes_by_cat = load_volumes_by_category(events_dir)
	if volumes_by_cat:
		out_cat = repo_root / "outputs" / "log_volume_hist_by_category.png"
		# print a small summary
		print("\nVolumes by category (top counts):")
		counts = sorted(((k, len(v)) for k, v in volumes_by_cat.items()), key=lambda t: t[1], reverse=True)
		for k, c in counts[:12]:
			print(f"  {k}: {c}")

		plot_log_hist_by_category(volumes_by_cat, out_cat)
		print(f"Saved category histogram to {out_cat}")

	# Yes outcome prices by category
	yes_by_cat = load_yes_prices_by_category(events_dir)
	if yes_by_cat:
		out_yes = repo_root / "outputs" / "yes_price_by_category.png"
		print("\nYes outcome price counts (top categories):")
		counts = sorted(((k, len(v)) for k, v in yes_by_cat.items()), key=lambda t: t[1], reverse=True)
		for k, c in counts[:16]:
			print(f"  {k}: {c}")

		# print a more detailed numeric summary for the top few categories
		for k, c in counts[:8]:
			vals = np.array(yes_by_cat[k], dtype=float)
			print(f"\n{k}: n={vals.size}, mean={vals.mean():.3f}, median={np.median(vals):.3f}, min={vals.min():.3f}, max={vals.max():.3f}")

		plot_yes_price_by_category(yes_by_cat, out_yes)
		print(f"Saved Yes-price boxplot to {out_yes}")


if __name__ == "__main__":
	main()

