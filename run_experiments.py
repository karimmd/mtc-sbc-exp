#!/usr/bin/env python3

import asyncio
import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "experiments"))

from experiments.run_evaluations import main

if __name__ == "__main__":
    print("Blockchain Blockchain: Blockchain Experimental Framework")
    print("=" * 80)
    print("Starting evaluation experiments...")
    print()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExperiment interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError running experiments: {e}")
        sys.exit(1)