#!/usr/bin/env python3
"""
Analyze arb_finder data logs for pattern discovery and model training.

This script reads JSONL logs produced by the Rust arb_finder and:
1. Calculates signal effectiveness (which signals predict arbs best)
2. Finds optimal weight configurations
3. Discovers time-based patterns
4. Exports features for ML training

Usage:
    python analyze_arb_data.py <log_dir>
    python analyze_arb_data.py arb_data/

Output:
    - Console summary of findings
    - learned_weights.json with optimized weights
    - features.csv for ML training (if enough data)
"""

import json
import sys
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import statistics


@dataclass
class Event:
    """Parsed log event"""
    type: str
    timestamp_ms: int
    market_id: str
    data: Dict[str, Any]


@dataclass
class PredictionWindow:
    """A prediction and whether an arb followed"""
    prediction_ts: int
    market_id: str
    confidence: float
    signals: List[str]
    arb_occurred: bool = False
    time_to_arb_ms: Optional[int] = None


@dataclass
class SignalStats:
    """Statistics for a signal type"""
    appearances: int = 0
    successes: int = 0  # Present when arb occurred
    false_positives: int = 0  # Present but no arb
    
    @property
    def hit_rate(self) -> float:
        if self.appearances == 0:
            return 0.0
        return self.successes / self.appearances
    
    @property
    def precision(self) -> float:
        """How often signal presence means arb will occur"""
        total = self.successes + self.false_positives
        if total == 0:
            return 0.0
        return self.successes / total


def parse_jsonl(filepath: Path) -> List[Event]:
    """Parse a JSONL file into events"""
    events = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                event_type = data.pop('type', 'unknown')
                timestamp_ms = data.pop('timestamp_ms', 0)
                market_id = data.pop('market_id', '')
                events.append(Event(
                    type=event_type,
                    timestamp_ms=timestamp_ms,
                    market_id=market_id,
                    data=data
                ))
            except json.JSONDecodeError:
                continue
    return events


def load_all_events(log_dir: Path) -> List[Event]:
    """Load all events from all log files in directory"""
    events = []
    for filepath in sorted(log_dir.glob('arb_data_*.jsonl')):
        print(f"Loading {filepath.name}...")
        events.extend(parse_jsonl(filepath))
    
    # Sort by timestamp
    events.sort(key=lambda e: e.timestamp_ms)
    return events


def analyze_signals(events: List[Event], window_ms: int = 2000) -> Dict[str, SignalStats]:
    """
    Analyze which signals predict arb opportunities.
    
    For each signal, we look ahead within window_ms to see if an arb occurred.
    """
    signal_stats: Dict[str, SignalStats] = defaultdict(SignalStats)
    
    # Find all arb opportunity timestamps per market
    arb_times: Dict[str, List[int]] = defaultdict(list)
    for event in events:
        if event.type == 'ArbOpportunityDetected':
            arb_times[event.market_id].append(event.timestamp_ms)
    
    # Analyze signals
    for event in events:
        if event.type == 'SignalDetected':
            signal_type = event.data.get('signal_type', 'unknown')
            market_id = event.market_id
            ts = event.timestamp_ms
            
            stats = signal_stats[signal_type]
            stats.appearances += 1
            
            # Check if arb occurred within window
            arb_occurred = any(
                0 <= (arb_ts - ts) <= window_ms
                for arb_ts in arb_times.get(market_id, [])
            )
            
            if arb_occurred:
                stats.successes += 1
            else:
                stats.false_positives += 1
    
    return dict(signal_stats)


def analyze_predictions(events: List[Event], window_ms: int = 2000) -> List[PredictionWindow]:
    """
    Match predictions to outcomes.
    """
    predictions = []
    
    # Find all arb opportunity timestamps per market
    arb_times: Dict[str, List[int]] = defaultdict(list)
    for event in events:
        if event.type == 'ArbOpportunityDetected':
            arb_times[event.market_id].append(event.timestamp_ms)
    
    # Process predictions
    for event in events:
        if event.type == 'Prediction':
            market_id = event.market_id
            ts = event.timestamp_ms
            confidence = float(event.data.get('confidence', 0))
            
            # Check if arb occurred within window
            matching_arbs = [
                arb_ts for arb_ts in arb_times.get(market_id, [])
                if 0 <= (arb_ts - ts) <= window_ms
            ]
            
            arb_occurred = len(matching_arbs) > 0
            time_to_arb = min(a - ts for a in matching_arbs) if matching_arbs else None
            
            predictions.append(PredictionWindow(
                prediction_ts=ts,
                market_id=market_id,
                confidence=confidence,
                signals=[],  # Would need to track from signal events
                arb_occurred=arb_occurred,
                time_to_arb_ms=time_to_arb
            ))
    
    return predictions


def calculate_optimal_weights(signal_stats: Dict[str, SignalStats]) -> Dict[str, float]:
    """
    Calculate optimal weights based on signal effectiveness.
    
    Uses hit rate and precision to weight signals.
    """
    weights = {}
    total_score = 0.0
    
    for signal_type, stats in signal_stats.items():
        # Score combines hit rate and precision
        # Hit rate: how often does the signal appear when arbs happen?
        # Precision: when the signal fires, how often does an arb follow?
        score = stats.hit_rate * 0.5 + stats.precision * 0.5
        weights[signal_type] = score
        total_score += score
    
    # Normalize to sum to 1.0
    if total_score > 0:
        for k in weights:
            weights[k] /= total_score
    
    return weights


def analyze_time_patterns(events: List[Event]) -> Dict[str, Any]:
    """
    Analyze time-based patterns in arb occurrences.
    """
    arb_hours = defaultdict(int)
    arb_minutes_within_hour = defaultdict(int)
    
    for event in events:
        if event.type == 'ArbOpportunityDetected':
            # Convert timestamp to datetime
            dt = datetime.fromtimestamp(event.timestamp_ms / 1000)
            arb_hours[dt.hour] += 1
            arb_minutes_within_hour[dt.minute] += 1
    
    return {
        'by_hour': dict(arb_hours),
        'by_minute': dict(arb_minutes_within_hour),
        'peak_hours': sorted(arb_hours.keys(), key=lambda h: arb_hours[h], reverse=True)[:3],
    }


def print_summary(
    signal_stats: Dict[str, SignalStats],
    predictions: List[PredictionWindow],
    time_patterns: Dict[str, Any],
    optimal_weights: Dict[str, float]
):
    """Print analysis summary"""
    print("\n" + "=" * 60)
    print("ARB FINDER DATA ANALYSIS")
    print("=" * 60)
    
    # Signal effectiveness
    print("\n📊 SIGNAL EFFECTIVENESS")
    print("-" * 40)
    for signal_type, stats in sorted(signal_stats.items(), key=lambda x: x[1].hit_rate, reverse=True):
        print(f"  {signal_type}:")
        print(f"    Appearances: {stats.appearances}")
        print(f"    Hit Rate:    {stats.hit_rate:.1%}")
        print(f"    Precision:   {stats.precision:.1%}")
    
    # Prediction accuracy
    if predictions:
        print("\n📈 PREDICTION ACCURACY")
        print("-" * 40)
        total = len(predictions)
        hits = sum(1 for p in predictions if p.arb_occurred)
        hit_rate = hits / total if total > 0 else 0
        print(f"  Total Predictions: {total}")
        print(f"  Successful:        {hits}")
        print(f"  Hit Rate:          {hit_rate:.1%}")
        
        # By confidence bucket
        confidence_buckets = defaultdict(lambda: {'total': 0, 'hits': 0})
        for p in predictions:
            bucket = int(p.confidence * 10) / 10  # Round to 0.1
            confidence_buckets[bucket]['total'] += 1
            if p.arb_occurred:
                confidence_buckets[bucket]['hits'] += 1
        
        print("\n  By Confidence Level:")
        for conf in sorted(confidence_buckets.keys()):
            data = confidence_buckets[conf]
            rate = data['hits'] / data['total'] if data['total'] > 0 else 0
            print(f"    {conf:.1f}: {data['hits']}/{data['total']} ({rate:.1%})")
        
        # Time to arb
        times = [p.time_to_arb_ms for p in predictions if p.time_to_arb_ms is not None]
        if times:
            print(f"\n  Average Time to Arb: {statistics.mean(times):.0f}ms")
            print(f"  Median Time to Arb:  {statistics.median(times):.0f}ms")
    
    # Time patterns
    print("\n⏰ TIME PATTERNS")
    print("-" * 40)
    if time_patterns['peak_hours']:
        print(f"  Peak Hours (UTC): {time_patterns['peak_hours']}")
    
    # Optimal weights
    print("\n⚖️  OPTIMAL WEIGHTS (for learned_weights.json)")
    print("-" * 40)
    for signal_type, weight in sorted(optimal_weights.items(), key=lambda x: x[1], reverse=True):
        print(f"  {signal_type}: {weight:.3f}")
    
    print("\n" + "=" * 60)


def save_weights(weights: Dict[str, float], output_path: Path):
    """Save optimal weights to JSON file"""
    # Map to the expected format
    weight_data = {
        'sweep': str(weights.get('Sweep', 0.25)),
        'imbalance': str(weights.get('Imbalance', 0.25)),
        'velocity': str(weights.get('Velocity', 0.25)),
        'discrepancy': str(weights.get('Discrepancy', 0.25)),
    }
    
    with open(output_path, 'w') as f:
        json.dump(weight_data, f, indent=2)
    
    print(f"\nSaved optimal weights to {output_path}")


def export_features(events: List[Event], predictions: List[PredictionWindow], output_path: Path):
    """Export features for ML training"""
    if len(predictions) < 100:
        print(f"\nNot enough data for ML export (need 100+, have {len(predictions)})")
        return
    
    # Create feature rows
    rows = []
    header = ['timestamp', 'market_id', 'confidence', 'arb_occurred', 'time_to_arb_ms']
    
    for p in predictions:
        rows.append([
            p.prediction_ts,
            p.market_id,
            p.confidence,
            1 if p.arb_occurred else 0,
            p.time_to_arb_ms or ''
        ])
    
    # Write CSV
    with open(output_path, 'w') as f:
        f.write(','.join(header) + '\n')
        for row in rows:
            f.write(','.join(str(v) for v in row) + '\n')
    
    print(f"\nExported {len(rows)} rows to {output_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_arb_data.py <log_dir>")
        print("Example: python analyze_arb_data.py arb_data/")
        sys.exit(1)
    
    log_dir = Path(sys.argv[1])
    if not log_dir.exists():
        print(f"Error: Directory {log_dir} does not exist")
        sys.exit(1)
    
    print(f"Analyzing logs in {log_dir}...")
    
    # Load all events
    events = load_all_events(log_dir)
    print(f"Loaded {len(events)} events")
    
    if not events:
        print("No events found!")
        sys.exit(1)
    
    # Count event types
    type_counts = defaultdict(int)
    for event in events:
        type_counts[event.type] += 1
    print("\nEvent counts:")
    for t, c in sorted(type_counts.items()):
        print(f"  {t}: {c}")
    
    # Analyze
    signal_stats = analyze_signals(events)
    predictions = analyze_predictions(events)
    time_patterns = analyze_time_patterns(events)
    optimal_weights = calculate_optimal_weights(signal_stats)
    
    # Print summary
    print_summary(signal_stats, predictions, time_patterns, optimal_weights)
    
    # Save outputs
    save_weights(optimal_weights, log_dir / 'learned_weights.json')
    export_features(events, predictions, log_dir / 'features.csv')


if __name__ == '__main__':
    main()
