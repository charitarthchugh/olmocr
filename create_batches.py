import argparse
import os
import math

def create_batches(src_directory, batch_size, output_dir):
    """
    Creates batch files containing lists of PDF file paths.

    Args:
        src_directory (str): The source directory containing PDF files.
        batch_size (int): The maximum number of PDF files per batch.
        output_dir (str): The directory to save the batch files.

    Returns:
        tuple: A tuple containing:
            - list: A list of paths to the created batch files.
            - int: The total number of PDF files found.
    """
    all_pdf_files = []
    for root, _, files in os.walk(src_directory):
        for file in files:
            if file.lower().endswith('.pdf'):
                all_pdf_files.append(os.path.join(root, file))

    total_pdfs = len(all_pdf_files)
    if total_pdfs == 0:
        print(f"No PDF files found in {src_directory}.")
        return [], 0

    num_batches = math.ceil(total_pdfs / batch_size)
    batch_file_paths = []

    for i in range(num_batches):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, total_pdfs)
        current_batch = all_pdf_files[start_index:end_index]

        batch_filename = os.path.join(output_dir, f"batch_{i+1}.txt")
        with open(batch_filename, 'w') as f:
            for pdf_path in current_batch:
                f.write(f"{pdf_path}\n")
        batch_file_paths.append(batch_filename)
        print(f"Created batch file: {batch_filename} with {len(current_batch)} PDFs.")

    return batch_file_paths, total_pdfs

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create batch files for PDF processing.")
    parser.add_argument("--src", required=True, help="Source directory containing PDF files.")
    parser.add_argument("--batch_size", type=int, required=True, help="Number of PDFs per batch.")
    parser.add_argument("--output_dir", required=True, help="Directory to save batch files.")

    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    batch_files, total_count = create_batches(args.src, args.batch_size, args.output_dir)

    # Print the paths of created batch files and total count for the shell script to capture
    for bf in batch_files:
        print(f"BATCH_FILE_PATH:{bf}")
    print(f"TOTAL_PDFS_COUNT:{total_count}")