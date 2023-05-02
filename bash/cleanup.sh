# Set the input and output file paths
input_file="raw_data.csv"
output_file="clean_data.csv"

# Remove any leading or trailing whitespace from each line
sed 's/^[ \t]*//;s/[ \t]*$//' $input_file > $output_file

# Replace any commas within quoted fields with a placeholder
sed -i 's/","/,/g' $output_file

# Replace any newlines within quoted fields with a placeholder
sed -i 's/","/ /g' $output_file

# Remove the quotes around each field
sed -i 's/"//g' $output_file

# Replace the placeholder with the original comma separator
sed -i 's/,/","/g' $output_file

echo "Data cleaning and formatting complete. Output file: $output_file"

