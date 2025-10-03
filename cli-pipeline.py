#!/usr/bin/env python3
"""
Trade Processing Pipeline - Command Line Interface
Run the complete pipeline without Streamlit
"""

import os
import sys
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import Optional, Tuple
import traceback

# Add modules directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

# Import all modules
from modules.input_parser import InputParser
from modules.trade_parser import TradeParser
from modules.position_manager import PositionManager
from modules.trade_processor import TradeProcessor
from modules.output_generator import OutputGenerator
from modules.acm_mapper import ACMMapper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class TradePipeline:
    """Main pipeline processor"""
    
    def __init__(self, 
                 mapping_file: str = None,
                 schema_file: str = None,
                 output_dir: str = "output"):
        """
        Initialize pipeline
        
        Args:
            mapping_file: Path to futures mapping CSV
            schema_file: Path to ACM schema Excel
            output_dir: Output directory for results
        """
        self.mapping_file = self._find_file(
            mapping_file, 
            ["futures_mapping.csv", "futures mapping.csv", "data/futures_mapping.csv"],
            "mapping"
        )
        
        self.schema_file = self._find_file(
            schema_file,
            ["acm_schema.xlsx", "data/acm_schema.xlsx"],
            "ACM schema"
        )
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Create stage subdirectories
        self.stage1_dir = self.output_dir / "stage1"
        self.stage2_dir = self.output_dir / "stage2"
        self.stage1_dir.mkdir(exist_ok=True)
        self.stage2_dir.mkdir(exist_ok=True)
        
        # Store results
        self.stage1_results = {}
        self.stage2_results = {}
    
    def _find_file(self, specified_file: Optional[str], 
                   default_locations: list, file_type: str) -> Optional[str]:
        """Find a file from specified path or default locations"""
        if specified_file and Path(specified_file).exists():
            return specified_file
        
        for location in default_locations:
            if Path(location).exists():
                print(f"✓ Found {file_type} file: {location}")
                return location
        
        return None
    
    def run_stage1(self, position_file: str, trade_file: str) -> bool:
        """
        Run Stage 1: Strategy Processing
        
        Args:
            position_file: Path to position file
            trade_file: Path to trade file
            
        Returns:
            Success status
        """
        print(f"\n{Colors.HEADER}{'='*60}")
        print("STAGE 1: STRATEGY PROCESSING")
        print(f"{'='*60}{Colors.ENDC}\n")
        
        try:
            # Check mapping file
            if not self.mapping_file:
                print(f"{Colors.FAIL}✗ No mapping file found. Please specify with --mapping{Colors.ENDC}")
                return False
            
            # Parse positions
            print(f"{Colors.CYAN}→ Parsing position file...{Colors.ENDC}")
            input_parser = InputParser(self.mapping_file)
            positions = input_parser.parse_file(position_file)
            
            if not positions:
                print(f"{Colors.FAIL}✗ No positions found{Colors.ENDC}")
                return False
            
            print(f"{Colors.GREEN}✓ Parsed {len(positions)} positions ({input_parser.format_type} format){Colors.ENDC}")
            
            # Parse trades
            print(f"{Colors.CYAN}→ Parsing trade file...{Colors.ENDC}")
            trade_parser = TradeParser(self.mapping_file)
            
            # Read raw trade dataframe
            if trade_file.endswith('.csv'):
                trade_df = pd.read_csv(trade_file, header=None)
            else:
                trade_df = pd.read_excel(trade_file, header=None)
            
            trades = trade_parser.parse_trade_file(trade_file)
            
            if not trades:
                print(f"{Colors.FAIL}✗ No trades found{Colors.ENDC}")
                return False
            
            print(f"{Colors.GREEN}✓ Parsed {len(trades)} trades ({trade_parser.format_type} format){Colors.ENDC}")
            
            # Check for missing mappings
            missing_positions = len(input_parser.unmapped_symbols) if hasattr(input_parser, 'unmapped_symbols') else 0
            missing_trades = len(trade_parser.unmapped_symbols) if hasattr(trade_parser, 'unmapped_symbols') else 0
            
            if missing_positions > 0 or missing_trades > 0:
                print(f"{Colors.WARNING}⚠ Found unmapped symbols: {missing_positions} from positions, {missing_trades} from trades{Colors.ENDC}")
            
            # Process trades
            print(f"{Colors.CYAN}→ Processing trades with strategy assignment...{Colors.ENDC}")
            position_manager = PositionManager()
            starting_positions_df = position_manager.initialize_from_positions(positions)
            
            trade_processor = TradeProcessor(position_manager)
            output_gen = OutputGenerator(str(self.stage1_dir))
            
            parsed_trades_df = output_gen.create_trade_dataframe_from_positions(trades)
            processed_trades_df = trade_processor.process_trades(trades, trade_df)
            final_positions_df = position_manager.get_final_positions()
            
            # Generate output files
            print(f"{Colors.CYAN}→ Saving Stage 1 outputs...{Colors.ENDC}")
            output_files = output_gen.save_all_outputs(
                parsed_trades_df,
                starting_positions_df,
                processed_trades_df,
                final_positions_df,
                file_prefix="stage1",
                input_parser=input_parser,
                trade_parser=trade_parser
            )
            
            # Store results
            self.stage1_results = {
                'processed_trades': processed_trades_df,
                'starting_positions': starting_positions_df,
                'final_positions': final_positions_df,
                'output_files': output_files
            }
            
            # Print summary
            print(f"\n{Colors.BOLD}Stage 1 Summary:{Colors.ENDC}")
            print(f"  • Starting positions: {len(starting_positions_df)}")
            print(f"  • Trades processed: {len(trades)}")
            
            if 'Strategy' in processed_trades_df.columns:
                fulo_count = len(processed_trades_df[processed_trades_df['Strategy'] == 'FULO'])
                fush_count = len(processed_trades_df[processed_trades_df['Strategy'] == 'FUSH'])
                print(f"  • FULO trades: {fulo_count}")
                print(f"  • FUSH trades: {fush_count}")
            
            if 'Split?' in processed_trades_df.columns:
                split_count = len(processed_trades_df[processed_trades_df['Split?'] == 'Yes'])
                if split_count > 0:
                    print(f"  • Split trades: {split_count}")
            
            print(f"  • Final positions: {len(final_positions_df)}")
            
            print(f"\n{Colors.GREEN}✓ Stage 1 Complete!{Colors.ENDC}")
            print(f"  Output directory: {self.stage1_dir}")
            
            return True
            
        except Exception as e:
            print(f"{Colors.FAIL}✗ Error in Stage 1: {str(e)}{Colors.ENDC}")
            logger.error(traceback.format_exc())
            return False
    
    def run_stage2(self, skip_stage1: bool = False) -> bool:
        """
        Run Stage 2: ACM Mapping
        
        Args:
            skip_stage1: If True, look for existing Stage 1 output
            
        Returns:
            Success status
        """
        print(f"\n{Colors.HEADER}{'='*60}")
        print("STAGE 2: ACM MAPPING")
        print(f"{'='*60}{Colors.ENDC}\n")
        
        try:
            # Check schema file
            if not self.schema_file:
                print(f"{Colors.FAIL}✗ No ACM schema file found. Please specify with --schema{Colors.ENDC}")
                return False
            
            # Get processed trades from Stage 1
            processed_trades_df = None
            
            if skip_stage1:
                # Look for existing Stage 1 output
                print(f"{Colors.CYAN}→ Looking for existing Stage 1 output...{Colors.ENDC}")
                stage1_files = list(self.stage1_dir.glob("stage1_3_processed_trades_*.csv"))
                
                if stage1_files:
                    # Use the most recent file
                    latest_file = max(stage1_files, key=lambda x: x.stat().st_mtime)
                    processed_trades_df = pd.read_csv(latest_file)
                    print(f"{Colors.GREEN}✓ Found Stage 1 output: {latest_file.name}{Colors.ENDC}")
                else:
                    print(f"{Colors.FAIL}✗ No Stage 1 output found. Please run Stage 1 first.{Colors.ENDC}")
                    return False
            else:
                # Use results from current run
                if 'processed_trades' not in self.stage1_results:
                    print(f"{Colors.FAIL}✗ Stage 1 must be completed first{Colors.ENDC}")
                    return False
                processed_trades_df = self.stage1_results['processed_trades']
            
            # Initialize ACM Mapper
            print(f"{Colors.CYAN}→ Loading ACM schema...{Colors.ENDC}")
            acm_mapper = ACMMapper(self.schema_file)
            
            if not acm_mapper.columns_order:
                print(f"{Colors.FAIL}✗ Failed to load ACM schema{Colors.ENDC}")
                return False
            
            print(f"{Colors.GREEN}✓ Loaded schema with {len(acm_mapper.columns_order)} columns{Colors.ENDC}")
            
            # Process to ACM format
            print(f"{Colors.CYAN}→ Mapping to ACM format...{Colors.ENDC}")
            mapped_df, errors_df = acm_mapper.process_trades_to_acm(processed_trades_df)
            
            # Save outputs
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            acm_file = self.stage2_dir / f"acm_listedtrades_{timestamp}.csv"
            mapped_df.to_csv(acm_file, index=False)
            
            errors_file = self.stage2_dir / f"acm_listedtrades_{timestamp}_errors.csv"
            errors_df.to_csv(errors_file, index=False)
            
            # Store results
            self.stage2_results = {
                'mapped': mapped_df,
                'errors': errors_df,
                'output_files': {
                    'acm_mapped': acm_file,
                    'errors': errors_file
                }
            }
            
            # Print summary
            print(f"\n{Colors.BOLD}Stage 2 Summary:{Colors.ENDC}")
            print(f"  • Records mapped: {len(mapped_df)}")
            print(f"  • Validation errors: {len(errors_df)}")
            
            if 'Transaction Type' in mapped_df.columns:
                trans_types = mapped_df['Transaction Type'].value_counts()
                for trans_type, count in trans_types.items():
                    print(f"  • {trans_type}: {count}")
            
            if len(errors_df) == 0:
                print(f"\n{Colors.GREEN}✓ Stage 2 Complete! No validation errors.{Colors.ENDC}")
            else:
                print(f"\n{Colors.WARNING}✓ Stage 2 Complete with {len(errors_df)} validation errors.{Colors.ENDC}")
                print(f"  Check error file: {errors_file.name}")
            
            print(f"  Output directory: {self.stage2_dir}")
            
            return True
            
        except Exception as e:
            print(f"{Colors.FAIL}✗ Error in Stage 2: {str(e)}{Colors.ENDC}")
            logger.error(traceback.format_exc())
            return False
    
    def run_complete_pipeline(self, position_file: str, trade_file: str) -> bool:
        """
        Run complete pipeline (both stages)
        
        Args:
            position_file: Path to position file
            trade_file: Path to trade file
            
        Returns:
            Success status
        """
        print(f"\n{Colors.BOLD}{Colors.BLUE}RUNNING COMPLETE PIPELINE{Colors.ENDC}")
        
        # Run Stage 1
        if not self.run_stage1(position_file, trade_file):
            return False
        
        # Run Stage 2
        if not self.run_stage2():
            return False
        
        print(f"\n{Colors.GREEN}{Colors.BOLD}✓ PIPELINE COMPLETE!{Colors.ENDC}")
        print(f"\nAll outputs saved to: {self.output_dir}")
        
        return True


def get_user_input(prompt: str, file_type: str = "file") -> str:
    """Get file path from user with validation"""
    while True:
        path = input(f"{Colors.CYAN}{prompt}{Colors.ENDC} ").strip()
        
        # Handle quotes
        if path.startswith('"') and path.endswith('"'):
            path = path[1:-1]
        if path.startswith("'") and path.endswith("'"):
            path = path[1:-1]
        
        # Check if file exists
        if Path(path).exists():
            return path
        else:
            print(f"{Colors.WARNING}File not found: {path}{Colors.ENDC}")
            retry = input("Try again? (y/n): ").lower()
            if retry != 'y':
                return None


def interactive_mode():
    """Interactive mode with prompts"""
    print(f"\n{Colors.BOLD}Trade Processing Pipeline - Interactive Mode{Colors.ENDC}")
    print("="*60)
    
    # Get input files
    print("\n📁 Please provide input files:")
    
    position_file = get_user_input("Enter position file path (Excel/CSV):")
    if not position_file:
        print(f"{Colors.FAIL}Exiting...{Colors.ENDC}")
        return
    
    trade_file = get_user_input("Enter trade file path (Excel/CSV):")
    if not trade_file:
        print(f"{Colors.FAIL}Exiting...{Colors.ENDC}")
        return
    
    # Optional files
    print("\n📄 Optional files (press Enter to use defaults):")
    
    mapping_input = input(f"{Colors.CYAN}Enter mapping file path (or Enter for default):{Colors.ENDC} ").strip()
    mapping_file = mapping_input if mapping_input and Path(mapping_input).exists() else None
    
    schema_input = input(f"{Colors.CYAN}Enter ACM schema file path (or Enter for default):{Colors.ENDC} ").strip()
    schema_file = schema_input if schema_input and Path(schema_input).exists() else None
    
    # Output directory
    output_input = input(f"{Colors.CYAN}Enter output directory (or Enter for './output'):{Colors.ENDC} ").strip()
    output_dir = output_input if output_input else "output"
    
    # Run mode
    print("\n🚀 Select processing mode:")
    print("1. Complete Pipeline (both stages)")
    print("2. Stage 1 only (Strategy Processing)")
    print("3. Stage 2 only (ACM Mapping - requires existing Stage 1 output)")
    
    mode = input(f"{Colors.CYAN}Enter choice (1/2/3):{Colors.ENDC} ").strip()
    
    # Initialize pipeline
    pipeline = TradePipeline(
        mapping_file=mapping_file,
        schema_file=schema_file,
        output_dir=output_dir
    )
    
    # Run based on mode
    if mode == '1':
        pipeline.run_complete_pipeline(position_file, trade_file)
    elif mode == '2':
        pipeline.run_stage1(position_file, trade_file)
    elif mode == '3':
        pipeline.run_stage2(skip_stage1=True)
    else:
        print(f"{Colors.FAIL}Invalid choice{Colors.ENDC}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Trade Processing Pipeline - Command Line Interface",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode
  python pipeline_cli.py
  
  # Complete pipeline
  python pipeline_cli.py -p positions.xlsx -t trades.csv
  
  # Stage 1 only
  python pipeline_cli.py -p positions.xlsx -t trades.csv --stage1
  
  # Stage 2 only (using existing Stage 1 output)
  python pipeline_cli.py --stage2
  
  # With custom files
  python pipeline_cli.py -p positions.xlsx -t trades.csv -m mapping.csv -s schema.xlsx -o output/
        """
    )
    
    parser.add_argument('-p', '--positions', type=str, help='Position file (Excel/CSV)')
    parser.add_argument('-t', '--trades', type=str, help='Trade file (Excel/CSV)')
    parser.add_argument('-m', '--mapping', type=str, help='Mapping file (CSV)')
    parser.add_argument('-s', '--schema', type=str, help='ACM schema file (Excel)')
    parser.add_argument('-o', '--output', type=str, default='output', help='Output directory')
    parser.add_argument('--stage1', action='store_true', help='Run Stage 1 only')
    parser.add_argument('--stage2', action='store_true', help='Run Stage 2 only')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # If no arguments, run interactive mode
    if not args.positions and not args.trades and not args.stage2:
        interactive_mode()
        return
    
    # Initialize pipeline
    pipeline = TradePipeline(
        mapping_file=args.mapping,
        schema_file=args.schema,
        output_dir=args.output
    )
    
    # Run based on arguments
    if args.stage2:
        # Stage 2 only
        pipeline.run_stage2(skip_stage1=True)
    elif args.stage1:
        # Stage 1 only
        if not args.positions or not args.trades:
            print(f"{Colors.FAIL}Error: Stage 1 requires both position and trade files{Colors.ENDC}")
            sys.exit(1)
        pipeline.run_stage1(args.positions, args.trades)
    else:
        # Complete pipeline
        if not args.positions or not args.trades:
            print(f"{Colors.FAIL}Error: Pipeline requires both position and trade files{Colors.ENDC}")
            sys.exit(1)
        pipeline.run_complete_pipeline(args.positions, args.trades)


if __name__ == "__main__":
    main()