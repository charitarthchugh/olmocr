#!/usr/bin/env bash
source ~/miniconda3/etc/profile.d/conda.sh
conda activate olmocr
# --- Configuration ---
BATCH_SIZE=400
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/1372057074736828456/jvz6OTCFDs1y0BQceTkIFJ-JFQ9B4Gdx0F80-95DzBNCEd3a3n2vs3v2DKfENUgOkvIV" # Replace with your actual webhook URL
TEST_MODE_JOB_LIMIT=1

# --- Default States ---
SRC=""
DEST=""
ENABLE_DISCORD_NOTIFICATIONS=false # Disabled by default
TEST_MODE=false

# --- Argument Parsing ---
declare -a POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --enable-discord)
            ENABLE_DISCORD_NOTIFICATIONS=true
            shift # past argument
            ;;
        --test)
            TEST_MODE=true
            shift # past argument
            ;;
        *)
            POSITIONAL_ARGS+=("$1") # save it for positional assignment
            shift # past argument
            ;;
    esac
done

# Assign positional arguments (SRC and DEST)
if [ "${#POSITIONAL_ARGS[@]}" -ne 2 ]; then
  echo "Usage: $0 <src_directory> <dest_directory> [--test] [--enable-discord]"
  echo ""
  echo "Arguments:"
  echo "  <src_directory>          Source directory containing PDF files."
  echo "  <dest_directory>         Destination directory for output files."
  echo ""
  echo "Options:"
  echo "  --test                   Enable test mode: Submits a maximum of $TEST_MODE_JOB_LIMIT Slurm jobs."
  echo "  --enable-discord         Enable Discord notifications (disabled by default)."
  exit 1
fi

SRC="${POSITIONAL_ARGS[0]}"
DEST="${POSITIONAL_ARGS[1]}"

# --- Initial Status Messages ---
if [ "$TEST_MODE" = true ]; then
  echo ">>> TEST MODE ENABLED: Will submit a maximum of $TEST_MODE_JOB_LIMIT jobs. <<<"
fi
if [ "$ENABLE_DISCORD_NOTIFICATIONS" = true ]; then
  echo ">>> Discord notifications ENABLED. <<<"
else
  echo ">>> Discord notifications DISABLED (use --enable-discord to activate). <<<"
fi

# --- Function Definitions ---
send_discord_notification() {
  if [ "$ENABLE_DISCORD_NOTIFICATIONS" = false ]; then
    return 0
  fi

  local message="$1"
  if [ -n "$DISCORD_WEBHOOK_URL" ] && [ "$DISCORD_WEBK_URL" != "YOUR_DISCORD_WEBHOOK_URL" ]; then
    curl -s -H "Content-Type: application/json" -X POST -d "{\"content\": \"$message\"}" "$DISCORD_WEBHOOK_URL"
  else
    # Only print if they were meant to be on but URL is bad
    echo "Discord webhook URL not configured or invalid. Skipping notification."
  fi
}

# --- Directory Setup ---
LOGS_DIR="./logs"
mkdir -p "$LOGS_DIR"
LOGS_DIR_ABS=$(realpath "$LOGS_DIR")

if [ ! -d "$SRC" ]; then
  echo "Error: Source directory '$SRC' does not exist or is not a directory."
  exit 1
fi
mkdir -p "$DEST"

src_basename=$(basename "$SRC")
dest_output_dir="$DEST/$src_basename"
mkdir -p "$dest_output_dir"
DEST_OUTPUT_DIR_ABS=$(realpath "$dest_output_dir")

main_log_file="$LOGS_DIR_ABS/${src_basename}_main.log"

# --- Main Script Logic ---
echo "Processing directory: $SRC for Slurm job submission" | tee -a "$main_log_file"
discord_start_message="Started Slurm job submission for directory: $src_basename"
if [ "$TEST_MODE" = true ]; then
    discord_start_message="$discord_start_message (TEST MODE - Max $TEST_MODE_JOB_LIMIT jobs)"
fi
send_discord_notification "$discord_start_message"

# --- Create batches using Python script ---
BATCH_FILES_TEMP_DIR=$(mktemp -d "${LOGS_DIR_ABS}/${src_basename}_batches_XXXXXX")
echo "Creating PDF batches using Python script in $BATCH_FILES_TEMP_DIR..." | tee -a "$main_log_file"

# Run the Python script and capture its output
python_output=$(python create_batches.py --src "$SRC" --batch_size "$BATCH_SIZE" --output_dir "$BATCH_FILES_TEMP_DIR")
python_exit_code=$?

if [ $python_exit_code -ne 0 ]; then
  echo "Error: Python script 'create_batches.py' failed with exit code $python_exit_code." | tee -a "$main_log_file"
  send_discord_notification "Error: PDF batch creation failed for $src_basename."
  exit 1
fi

declare -a all_batch_files=()
total_pdfs=0

# Parse the output from the Python script
while IFS= read -r line; do
  if [[ "$line" =~ ^BATCH_FILE_PATH:(.*)$ ]]; then
    all_batch_files+=("${BASH_REMATCH[1]}")
  elif [[ "$line" =~ ^TOTAL_PDFS_COUNT:([0-9]+)$ ]]; then
    total_pdfs="${BASH_REMATCH[1]}"
  fi
done <<< "$python_output"

total_batches=${#all_batch_files[@]}

if [ "$total_pdfs" -eq 0 ]; then
  echo "No PDF files found in $SRC. Nothing to submit." | tee -a "$main_log_file"
  send_discord_notification "No PDF files found in $src_basename. No Slurm jobs submitted."
  echo "All processing complete. No PDFs found in $SRC." | tee -a "$LOGS_DIR_ABS/master_conversion_summary.log"
  rm -rf "$BATCH_FILES_TEMP_DIR" # Clean up temporary batch files
  exit 0
fi

echo "Found $total_pdfs PDF(s) in $SRC. Submitting Slurm jobs in $total_batches batches of up to $BATCH_SIZE." | tee -a "$main_log_file"
actual_jobs_submitted_count=0

for (( i=0; i<total_batches; i++ )); do
  batch_num=$((i + 1))
  current_batch_file="${all_batch_files[$i]}"

  if [ "$TEST_MODE" = true ] && [ "$batch_num" -gt "$TEST_MODE_JOB_LIMIT" ]; then
    echo "TEST MODE: Reached job limit of $TEST_MODE_JOB_LIMIT (current batch number is $batch_num). Halting further submissions." | tee -a "$main_log_file"
    send_discord_notification "TEST MODE: Submission limit $TEST_MODE_JOB_LIMIT reached for $src_basename. Halting."
    break
  fi
  actual_jobs_submitted_count=$((actual_jobs_submitted_count + 1))

  echo "Preparing batch $batch_num / $total_batches for $src_basename using file: $current_batch_file" | tee -a "$main_log_file"

  # Recalculate used_nodes for each submission to ensure it's up-to-date
  echo "Checking for nodes currently in use by your jobs before submitting batch $batch_num..." | tee -a "$main_log_file"
  # -h: no header, -u $USER: for current user, -o "%N": output node list
  # sort -u: unique nodes, paste -sd,: comma separated
  used_nodes=$(squeue -h -u "$USER" -o "%N" | sort -u | paste -sd, -)

  EXCLUDE_OPTION=""
  if [ -n "$used_nodes" ]; then
    echo "Nodes currently in use by user $USER (excluding for this job): $used_nodes" | tee -a "$main_log_file"
    EXCLUDE_OPTION="#SBATCH --exclude=$used_nodes"
  else
    echo "No nodes currently in use by user $USER. No nodes will be excluded for this job." | tee -a "$main_log_file"
  fi


  slurm_job_file=$(mktemp "$LOGS_DIR_ABS/${src_basename}_batch_${batch_num}_slurmjob_XXXXXX.sh")

  cat <<EOF > "$slurm_job_file"
#!/bin/bash
#SBATCH --job-name=olmocr_${src_basename}_b${batch_num}
#SBATCH --ntasks=8
#SBATCH --partition=general-gpu
#SBATCH --mem=32G                  # Memory allocation
#SBATCH --output=${LOGS_DIR_ABS}/${src_basename}_batch_${batch_num}_slurm_%j.out
#SBATCH --error=${LOGS_DIR_ABS}/${src_basename}_batch_${batch_num}_slurm_%j.err
#SBATCH --exclusive
#SBATCH --constraint=a30
#SBATCH --mail-user=chc21001@uconn.edu
#SBATCH --mail-type=ALL


echo "Host: \$(hostname)"
echo "Time: \$(date)"
echo "Directory: \$(pwd)"
echo "Slurm Job ID: \$SLURM_JOB_ID"
echo "Slurm Job Name: \$SLURM_JOB_NAME"

echo "Starting Slurm job for batch ${batch_num} of ${src_basename}"
echo "Processing PDF list: ${current_batch_file}"
echo "Output directory: ${DEST_OUTPUT_DIR_ABS}"

# IMPORTANT: Activate your Python environment here if needed
# Example:
source ~/miniconda3/etc/profile.d/conda.sh
conda activate olmocr
# or
# module load python/your_version anaconda3/version etc.
module load gcc/11.3.0

mkdir -p "${DEST_OUTPUT_DIR_ABS}"

echo "Running olmocr.pipeline for batch ${batch_num} (${src_basename})..."
#export CUDA_VISIBLE_DEVICES=0 # Ensure this matches your Slurm GPU allocation strategy
cat "${current_batch_file}" | xargs python -m olmocr.pipeline "${DEST_OUTPUT_DIR_ABS}" --markdown  --pdfs


if [ \$? -eq 0 ]; then
  echo "Python script for batch ${batch_num} (${src_basename}) completed successfully."
else
  echo "Python script for batch ${batch_num} (${src_basename}) failed. Check Slurm error file: ${LOGS_DIR_ABS}/${src_basename}_batch_${batch_num}_slurm_\$SLURM_JOB_ID.err"
fi
echo "Slurm job for batch ${batch_num} of ${src_basename} finished."
EOF

  chmod +x "$slurm_job_file"

  echo "Submitting Slurm job for batch $batch_num ($src_basename) using script: $slurm_job_file" | tee -a "$main_log_file"
  sbatch_output=$(sbatch "$slurm_job_file")
  sbatch_exit_code=$?

  if [ $sbatch_exit_code -eq 0 ]; then
    job_id=$(echo "$sbatch_output" | awk '{print $NF}')
    echo "Slurm job for batch $batch_num ($src_basename) submitted successfully. Job ID: $job_id" | tee -a "$main_log_file"
    # send_discord_notification "Slurm job $job_id submitted for batch $batch_num ($src_basename)." # Individual job notifications commented out as per original
  else
    echo "Error submitting Slurm job for batch $batch_num ($src_basename). sbatch exit code: $sbatch_exit_code. Output: $sbatch_output" | tee -a "$main_log_file"
    # send_discord_notification "Error submitting Slurm job for batch $batch_num ($src_basename)." # Individual job notifications commented out
  fi
  echo "-----------------------------------------------------" | tee -a "$main_log_file"
done

completion_message_suffix=""
if [ "$TEST_MODE" = true ]; then
  if [ "$total_batches" -gt "$TEST_MODE_JOB_LIMIT" ] && [ "$actual_jobs_submitted_count" -eq "$TEST_MODE_JOB_LIMIT" ]; then
     completion_message_suffix=" (Test Mode Limit: $TEST_MODE_JOB_LIMIT reached)"
  elif [ "$total_batches" -le "$TEST_MODE_JOB_LIMIT" ] || [ "$actual_jobs_submitted_count" -lt "$total_batches" ]; then # Condition simplified for clarity
     completion_message_suffix=" (Test Mode, $actual_jobs_submitted_count of $total_batches potential batches submitted)"
  else # This case implies total_batches <= TEST_MODE_JOB_LIMIT and all were submitted
     completion_message_suffix=" (Test Mode, all $actual_jobs_submitted_count batches submitted)"
  fi
fi

send_discord_notification "All Slurm job submissions for directory $src_basename complete. $actual_jobs_submitted_count batch(es) submitted$completion_message_suffix for $total_pdfs PDF(s)."
echo "All Slurm job submissions complete for $SRC. $actual_jobs_submitted_count submitted$completion_message_suffix." | tee -a "$LOGS_DIR_ABS/master_conversion_summary.log"

#rm -rf "$BATCH_FILES_TEMP_DIR" # Clean up temporary batch files after all submissions