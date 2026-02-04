#!/usr/bin/fish
# ORCA MASTER CONTROL: The Synchronized Edition (V5.0 FIXED)
# Location: Root Project
# Purpose: Perfect synchronization with AdvancedStrategyPipeline

# --- 0. ENVIRONMENT VALIDATION (KOTOR & SUPERIOR) ---
set ENV_PYTHON "/home/bumip/miniconda3/envs/quant_lab/bin/python3"

# Verify Python exists
if not test -x "$ENV_PYTHON"
    set_color red --bold
    echo "⛔ PYTHON NOT FOUND: $ENV_PYTHON"
    echo "   Please ensure quant_lab environment exists:"
    echo "   conda activate quant_lab"
    set_color normal
    exit 1
end

# Set PYTHONPATH for absolute imports
set -gx PYTHONPATH (pwd) $PYTHONPATH

# --- 1. PARAMETER MAPPING (Synchronized with pipeline.py) ---
# Default values matching pipeline.py defaults
set TARGET     $argv[1]; or set TARGET "DOGE"
set ANCHOR     "BTC"   # Fixed as per pipeline.py
set START      "2024-01-01"
set END        "2024-12-31"

# Logic parameters (matching pipeline.py argument names)
set ENTRY_THRESH $argv[2]; or set ENTRY_THRESH "2.0"
set EXIT_THRESH  "0.5"    # Default from pipeline.py
set WARMUP_DAYS  $argv[3]; or set WARMUP_DAYS "30"

# Kalman parameters (matching pipeline.py argument names)
set PROCESS_NOISE $argv[4]; or set PROCESS_NOISE "1e-5"
set OBS_NOISE     "1e-4"   # Default from pipeline.py

function print_header
    echo ""
    set_color yellow
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    set_color cyan --bold
    echo "🛠️  $argv[1]"
    set_color normal
end

function print_command
    set_color magenta
    echo "📡 COMMAND: $argv[1]"
    set_color normal
end

function print_success
    set_color green
    echo "✅ $argv[1]"
    set_color normal
end

function print_error
    set_color red --bold
    echo "❌ $argv[1]"
    set_color normal
end

function print_warning
    set_color yellow
    echo "⚠️  $argv[1]"
    set_color normal
end

# --- 2. PRE-FLIGHT CHECK ---
print_header "PRE-FLIGHT CHECK"
echo "Target:          $TARGET"
echo "Anchor:          $ANCHOR"
echo "Period:          $START → $END"
echo "Entry Threshold: $ENTRY_THRESH"
echo "Exit Threshold:  $EXIT_THRESH"
echo "Warmup Days:     $WARMUP_DAYS"
echo "Process Noise:   $PROCESS_NOISE"
echo "Obs Noise:       $OBS_NOISE"
echo "Python:          $ENV_PYTHON"

# Check critical directories
set MISSING_DIRS ""
for dir in data/silver research/strategy research/analysis
    if not test -d "$dir"
        set MISSING_DIRS "$MISSING_DIRS\n  - $dir"
    end
end

if test -n "$MISSING_DIRS"
    print_warning "Missing directories:$MISSING_DIRS"
    echo "Creating missing directories..."
    
    for dir in data/silver research/strategy research/analysis
        mkdir -p "$dir" 2>/dev/null
    end
    
    # Create empty __init__.py files
    touch research/__init__.py 2>/dev/null
    touch research/strategy/__init__.py 2>/dev/null
    touch research/analysis/__init__.py 2>/dev/null
end

# --- 3. EXECUTION FLOW (Perfectly synchronized with pipeline.py) ---
print_header "PHASE 1: STRATEGY EXECUTION"
print_command "$ENV_PYTHON -m research.strategy.pipeline \
    --target $TARGET \
    --anchor $ANCHOR \
    --start $START \
    --end $END \
    --entry-threshold $ENTRY_THRESH \
    --exit-threshold $EXIT_THRESH \
    --warmup $WARMUP_DAYS \
    --process-noise $PROCESS_NOISE \
    --obs-noise $OBS_NOISE"

# Execute the pipeline
$ENV_PYTHON -m research.strategy.pipeline \
    --target $TARGET \
    --anchor $ANCHOR \
    --start $START \
    --end $END \
    --entry-threshold $ENTRY_THRESH \
    --exit-threshold $EXIT_THRESH \
    --warmup $WARMUP_DAYS \
    --process-noise $PROCESS_NOISE \
    --obs-noise $OBS_NOISE

set PIPELINE_STATUS $status

if test $PIPELINE_STATUS -ne 0
    print_error "Pipeline failed with status: $PIPELINE_STATUS"
    
    # Diagnostic help
    print_header "DIAGNOSTIC HELP"
    
    if not test -d "data/silver" -o (count (find data/silver -name "*.parquet" 2>/dev/null)) -eq 0
        echo "1. Missing silver data:"
        echo "   mkdir -p data/silver"
        echo "   # Add your parquet files here"
    end
    
    if not test -f "research/strategy/__init__.py"
        echo "2. Missing __init__.py files:"
        echo "   touch research/strategy/__init__.py"
    end
    
    echo "3. Check Python imports:"
    echo "   $ENV_PYTHON -c \"import research.strategy.pipeline\""
    
    set_color yellow
    echo -n "Continue with diagnostics? (y/n): "
    set_color normal
    read -l continue_choice
    
    if not test "$continue_choice" = "y" -o "$continue_choice" = "Y"
        exit 1
    end
else
    print_success "Pipeline execution completed"
end

# --- 4. POST-EXECUTION ANALYSIS ---
print_header "PHASE 2: SYSTEM VALIDATION"
print_command "$ENV_PYTHON research/analysis/sanity.py"

$ENV_PYTHON research/analysis/sanity.py
set SANITY_STATUS $status

if test $SANITY_STATUS -eq 0
    print_success "System validation: HEALTHY"
else if test $SANITY_STATUS -eq 2
    print_warning "System validation: NEEDS ATTENTION"
else
    print_error "System validation: FAILED"
end

# Find latest result file
set RESULTS_DIR "research/results"
set LATEST_FILE ""

if test -d $RESULTS_DIR
    set LATEST_FILE (ls -t $RESULTS_DIR/*.parquet 2>/dev/null | head -1)
end

if test -n "$LATEST_FILE" -a -f "$LATEST_FILE"
    set FILE_NAME (basename $LATEST_FILE)
    print_header "PHASE 3: RESULT ANALYSIS ($FILE_NAME)"
    
    # Inspector
    print_command "$ENV_PYTHON research/analysis/inspector.py --file $LATEST_FILE"
    $ENV_PYTHON research/analysis/inspector.py --file $LATEST_FILE
    
    # Visualizer (save only, no display)
    print_header "PHASE 4: VISUALIZATION"
    print_command "$ENV_PYTHON research/analysis/visualizer.py --file $LATEST_FILE --save-only --entry $ENTRY_THRESH --exit $EXIT_THRESH"
    $ENV_PYTHON research/analysis/visualizer.py --file $LATEST_FILE --save-only --entry $ENTRY_THRESH --exit $EXIT_THRESH 2>/dev/null
    
    if test $status -eq 0
        print_success "Visualization saved: $LATEST_FILE.png"
    else
        print_warning "Visualization failed (continuing anyway)"
    end
else
    print_header "PHASE 3: RESULT ANALYSIS"
    print_warning "No result files found for analysis"
    
    # Check debug directory as fallback
    set DEBUG_FILE "research/debug_data/latest_run.parquet"
    if test -f $DEBUG_FILE
        print_success "Found debug file: $DEBUG_FILE"
        print_command "$ENV_PYTHON research/analysis/inspector.py --file $DEBUG_FILE"
        $ENV_PYTHON research/analysis/inspector.py --file $DEBUG_FILE
    end
end

# --- 5. FINAL SUMMARY ---
print_header "MISSION SUMMARY"
echo "Configuration:"
echo "  • Target:        $TARGET"
echo "  • Anchor:        $ANCHOR"
echo "  • Entry:         $ENTRY_THRESH"
echo "  • Exit:          $EXIT_THRESH"
echo "  • Warmup:        $WARMUP_DAYS days"
echo "  • Process Noise: $PROCESS_NOISE"
echo "  • Obs Noise:     $OBS_NOISE"

echo ""
echo "Generated Artifacts:"

# Check for parquet files
if test -d $RESULTS_DIR
    set PARQUET_FILES (ls $RESULTS_DIR/*.parquet 2>/dev/null | wc -l)
    if test $PARQUET_FILES -gt 0
        set_color green
        echo "  ✅ $PARQUET_FILES result file(s) in research/results/"
        set_color normal
        
        # List latest 2 files - FIXED SYNTAX ERROR HERE
        for file in (ls -t $RESULTS_DIR/*.parquet | head -2)
            set size (stat -c %s $file 2>/dev/null; or stat -f %z $file 2>/dev/null)
            # FIX: Proper fish syntax for string concatenation
            if type -q numfmt
                set size_fmt (echo $size | numfmt --to=iec 2>/dev/null)
            else
                set size_fmt (echo $size"B")
            end
            echo "     • "(basename $file)" ($size_fmt)"
        end
    end
end

# Check for PNG files
if test -d $RESULTS_DIR
    set PNG_FILES (ls $RESULTS_DIR/*.png 2>/dev/null | wc -l)
    if test $PNG_FILES -gt 0
        set_color green
        echo "  ✅ $PNG_FILES visualization file(s)"
        set_color normal
    end
end

# Check for metadata files
if test -d $RESULTS_DIR
    set META_FILES (ls $RESULTS_DIR/*.json 2>/dev/null | wc -l)
    if test $META_FILES -gt 0
        set_color green
        echo "  ✅ $META_FILES metadata file(s)"
        set_color normal
    end
end

# Check logs
if test -d "logs/strategy_pipeline"
    set LOG_FILES (ls logs/strategy_pipeline/*.log 2>/dev/null | wc -l)
    if test $LOG_FILES -gt 0
        echo "  ✅ $LOG_FILES log file(s) in logs/strategy_pipeline/"
    end
end

# Final message
echo ""
set_color cyan --bold
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎯 ORCA CYCLE COMPLETE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
set_color normal
echo ""
echo "Next commands:"
echo "  ./orca_cycle.fish DOGE 2.0 30 1e-5"
echo "  ./orca_cycle.fish ETH 1.8 45 1e-6"
echo "  ./orca_cycle.fish SOL 2.2 60 1e-7"
#!/usr/bin/fish
# ORCA MASTER CONTROL: The Synchronized Edition (V5.1 FIXED)
# Location: Root Project
# Purpose: Perfect synchronization with AdvancedStrategyPipeline

# --- 0. ENVIRONMENT VALIDATION (KOTOR & SUPERIOR) ---
set ENV_PYTHON "/home/bumip/miniconda3/envs/quant_lab/bin/python3"

# Verify Python exists
if not test -x "$ENV_PYTHON"
    set_color red --bold
    echo "⛔ PYTHON NOT FOUND: $ENV_PYTHON"
    echo "   Please ensure quant_lab environment exists:"
    echo "   conda activate quant_lab"
    set_color normal
    exit 1
end

# Set PYTHONPATH for absolute imports
set -gx PYTHONPATH (pwd) $PYTHONPATH

# --- 1. PARAMETER MAPPING (Synchronized with pipeline.py) ---
# Default values matching pipeline.py defaults
set TARGET     $argv[1]; or set TARGET "DOGE"
set ANCHOR     "BTC"   # Fixed as per pipeline.py
set START      "2024-01-01"
set END        "2024-12-31"

# Logic parameters (matching pipeline.py argument names)
set ENTRY_THRESH $argv[2]; or set ENTRY_THRESH "2.0"
set EXIT_THRESH  "0.5"    # Default from pipeline.py
set WARMUP_DAYS  $argv[3]; or set WARMUP_DAYS "30"

# Kalman parameters (matching pipeline.py argument names)
set PROCESS_NOISE $argv[4]; or set PROCESS_NOISE "1e-5"
set OBS_NOISE     "1e-4"   # Default from pipeline.py

function print_header
    echo ""
    set_color yellow
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    set_color cyan --bold
    echo "🛠️  $argv[1]"
    set_color normal
end

function print_command
    set_color magenta
    echo "📡 COMMAND: $argv[1]"
    set_color normal
end

function print_success
    set_color green
    echo "✅ $argv[1]"
    set_color normal
end

function print_error
    set_color red --bold
    echo "❌ $argv[1]"
    set_color normal
end

function print_warning
    set_color yellow
    echo "⚠️  $argv[1]"
    set_color normal
end

# --- 2. PRE-FLIGHT CHECK ---
print_header "PRE-FLIGHT CHECK"
echo "Target:          $TARGET"
echo "Anchor:          $ANCHOR"
echo "Period:          $START → $END"
echo "Entry Threshold: $ENTRY_THRESH"
echo "Exit Threshold:  $EXIT_THRESH"
echo "Warmup Days:     $WARMUP_DAYS"
echo "Process Noise:   $PROCESS_NOISE"
echo "Obs Noise:       $OBS_NOISE"
echo "Python:          $ENV_PYTHON"

# Check critical directories
set MISSING_DIRS ""
for dir in data/silver research/strategy research/analysis
    if not test -d "$dir"
        set MISSING_DIRS "$MISSING_DIRS\n  - $dir"
    end
end

if test -n "$MISSING_DIRS"
    print_warning "Missing directories:$MISSING_DIRS"
    echo "Creating missing directories..."
    
    for dir in data/silver research/strategy research/analysis
        mkdir -p "$dir" 2>/dev/null
    end
    
    # Create empty __init__.py files
    touch research/__init__.py 2>/dev/null
    touch research/strategy/__init__.py 2>/dev/null
    touch research/analysis/__init__.py 2>/dev/null
end

# --- 3. EXECUTION FLOW (Perfectly synchronized with pipeline.py) ---
print_header "PHASE 1: STRATEGY EXECUTION"
print_command "$ENV_PYTHON -m research.strategy.pipeline \
    --target $TARGET \
    --anchor $ANCHOR \
    --start $START \
    --end $END \
    --entry-threshold $ENTRY_THRESH \
    --exit-threshold $EXIT_THRESH \
    --warmup $WARMUP_DAYS \
    --process-noise $PROCESS_NOISE \
    --obs-noise $OBS_NOISE"

# Execute the pipeline
$ENV_PYTHON -m research.strategy.pipeline \
    --target $TARGET \
    --anchor $ANCHOR \
    --start $START \
    --end $END \
    --entry-threshold $ENTRY_THRESH \
    --exit-threshold $EXIT_THRESH \
    --warmup $WARMUP_DAYS \
    --process-noise $PROCESS_NOISE \
    --obs-noise $OBS_NOISE

set PIPELINE_STATUS $status

if test $PIPELINE_STATUS -ne 0
    print_error "Pipeline failed with status: $PIPELINE_STATUS"
    
    # Diagnostic help
    print_header "DIAGNOSTIC HELP"
    
    if not test -d "data/silver" -o (count (find data/silver -name "*.parquet" 2>/dev/null)) -eq 0
        echo "1. Missing silver data:"
        echo "   mkdir -p data/silver"
        echo "   # Add your parquet files here"
    end
    
    if not test -f "research/strategy/__init__.py"
        echo "2. Missing __init__.py files:"
        echo "   touch research/strategy/__init__.py"
    end
    
    echo "3. Check Python imports:"
    echo "   $ENV_PYTHON -c \"import research.strategy.pipeline\""
    
    set_color yellow
    echo -n "Continue with diagnostics? (y/n): "
    set_color normal
    read -l continue_choice
    
    if not test "$continue_choice" = "y" -o "$continue_choice" = "Y"
        exit 1
    end
else
    print_success "Pipeline execution completed"
end

# --- 4. POST-EXECUTION ANALYSIS ---
print_header "PHASE 2: SYSTEM VALIDATION"
print_command "$ENV_PYTHON research/analysis/sanity.py"

$ENV_PYTHON research/analysis/sanity.py
set SANITY_STATUS $status

if test $SANITY_STATUS -eq 0
    print_success "System validation: HEALTHY"
else if test $SANITY_STATUS -eq 2
    print_warning "System validation: NEEDS ATTENTION"
else
    print_error "System validation: FAILED"
end

# Find latest result file
set RESULTS_DIR "research/results"
set LATEST_FILE ""

if test -d $RESULTS_DIR
    # Use simple ls without -t option for compatibility
    set PARQUET_FILES (find $RESULTS_DIR -name "*.parquet" -type f 2>/dev/null)
    if test (count $PARQUET_FILES) -gt 0
        # Sort by modification time manually
        set LATEST_FILE ""
        set LATEST_TIME 0
        for file in $PARQUET_FILES
            set mtime (stat -c %Y "$file" 2>/dev/null; or stat -f %m "$file" 2>/dev/null)
            if test $mtime -gt $LATEST_TIME
                set LATEST_TIME $mtime
                set LATEST_FILE $file
            end
        end
    end
end

if test -n "$LATEST_FILE" -a -f "$LATEST_FILE"
    set FILE_NAME (basename $LATEST_FILE)
    print_header "PHASE 3: RESULT ANALYSIS ($FILE_NAME)"
    
    # Inspector
    print_command "$ENV_PYTHON research/analysis/inspector.py --file $LATEST_FILE"
    $ENV_PYTHON research/analysis/inspector.py --file $LATEST_FILE
    
    # Visualizer (save only, no display)
    print_header "PHASE 4: VISUALIZATION"
    print_command "$ENV_PYTHON research/analysis/visualizer.py --file $LATEST_FILE --save-only --entry $ENTRY_THRESH --exit $EXIT_THRESH"
    $ENV_PYTHON research/analysis/visualizer.py --file $LATEST_FILE --save-only --entry $ENTRY_THRESH --exit $EXIT_THRESH 2>/dev/null
    
    if test $status -eq 0
        print_success "Visualization saved: $LATEST_FILE.png"
    else
        print_warning "Visualization failed (continuing anyway)"
    end
else
    print_header "PHASE 3: RESULT ANALYSIS"
    print_warning "No result files found for analysis"
    
    # Check debug directory as fallback
    set DEBUG_FILE "research/debug_data/latest_run.parquet"
    if test -f $DEBUG_FILE
        print_success "Found debug file: $DEBUG_FILE"
        print_command "$ENV_PYTHON research/analysis/inspector.py --file $DEBUG_FILE"
        $ENV_PYTHON research/analysis/inspector.py --file $DEBUG_FILE
    end
end

# --- 5. FINAL SUMMARY ---
print_header "MISSION SUMMARY"
echo "Configuration:"
echo "  • Target:        $TARGET"
echo "  • Anchor:        $ANCHOR"
echo "  • Entry:         $ENTRY_THRESH"
echo "  • Exit:          $EXIT_THRESH"
echo "  • Warmup:        $WARMUP_DAYS days"
echo "  • Process Noise: $PROCESS_NOISE"
echo "  • Obs Noise:     $OBS_NOISE"

echo ""
echo "Generated Artifacts:"

# Check for parquet files
if test -d $RESULTS_DIR
    # Count parquet files safely
    set PARQUET_COUNT 0
    for file in $RESULTS_DIR/*.parquet
        if test -f "$file"
            set PARQUET_COUNT (math $PARQUET_COUNT + 1)
        end
    end
    
    if test $PARQUET_COUNT -gt 0
        set_color green
        echo "  ✅ $PARQUET_COUNT result file(s) in research/results/"
        set_color normal
        
        # List latest 2 files
        set COUNT 0
        for file in (find $RESULTS_DIR -name "*.parquet" -type f -exec stat -c "%Y %n" {} \; 2>/dev/null | sort -rn | cut -d' ' -f2- | head -2)
            if test -f "$file"
                set COUNT (math $COUNT + 1)
                set size (stat -c %s "$file" 2>/dev/null; or stat -f %z "$file" 2>/dev/null)
                # Format size
                if type -q numfmt
                    set size_fmt (echo $size | numfmt --to=iec 2>/dev/null)
                else if type -q python3
                    set size_fmt (python3 -c "
import math
size = $size
if size == 0: print('0B')
elif size < 1024: print(f'{size}B')
elif size < 1024**2: print(f'{size/1024:.1f}K')
elif size < 1024**3: print(f'{size/(1024**2):.1f}M')
else: print(f'{size/(1024**3):.1f}G')
")
                else
                    set size_fmt (echo $size"B")
                end
                echo "     • "(basename $file)" ($size_fmt)"
            end
        end
    else
        echo "  ⚠️  No result files in research/results/"
    end
end

# Check for PNG files - FIXED WILDCARD ERROR
echo ""
set PNG_COUNT 0
if test -d $RESULTS_DIR
    for file in $RESULTS_DIR/*.png
        if test -f "$file"
            set PNG_COUNT (math $PNG_COUNT + 1)
        end
    end
end

if test $PNG_COUNT -gt 0
    set_color green
    echo "  ✅ $PNG_COUNT visualization file(s)"
    set_color normal
else
    echo "  ⚠️  No visualization files (PNG) found"
end

# Check for metadata files
set META_COUNT 0
if test -d $RESULTS_DIR
    for file in $RESULTS_DIR/*.json
        if test -f "$file"
            set META_COUNT (math $META_COUNT + 1)
        end
    end
end

if test $META_COUNT -gt 0
    set_color green
    echo "  ✅ $META_COUNT metadata file(s)"
    set_color normal
end

# Check logs
set LOG_COUNT 0
if test -d "logs/strategy_pipeline"
    for file in logs/strategy_pipeline/*.log
        if test -f "$file"
            set LOG_COUNT (math $LOG_COUNT + 1)
        end
    end
end

if test $LOG_COUNT -gt 0
    echo "  ✅ $LOG_COUNT log file(s) in logs/strategy_pipeline/"
end

# Final message
echo ""
set_color cyan --bold
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎯 ORCA CYCLE COMPLETE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
set_color normal
echo ""
echo "Next commands:"
echo "  ./orca_cycle.fish DOGE 2.0 30 1e-5"
echo "  ./orca_cycle.fish ETH 1.8 45 1e-6"
echo "  ./orca_cycle.fish SOL 2.2 60 1e-7"
echo ""
