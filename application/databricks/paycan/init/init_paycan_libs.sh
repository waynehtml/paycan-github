run_command_with_retries() {
  local cmd="$1"
  local max_retries=${2:-2}  # Default to 2 retry if not provided
  local attempt=1
 
  # Execute the command and retry if it fails
  while [ $attempt -le $max_retries ]; do
    echo "Attempt $attempt: Running command: $cmd"
    eval "$cmd"
    
    # Capture the return value of the command
    local return_value=$?
    
    # Check if the command succeeded
    if [ $return_value -eq 0 ]; then
      echo "The command executed successfully on attempt $attempt."
      return 0
    fi
    
    # If the command failed, increment the attempt counter
    echo "The command failed with return value $return_value. Retrying..."
    attempt=$((attempt + 1))
  done
 
  # If the command failed in all attempts
  echo "The command failed after $max_retries attempts with return value $return_value."
  return $return_value
}
 
#
# Install library
#

pip config set global.trusted-host "fcp4lnexuap01.bocad.bank-banque-canada.ca"
pip config set global.index "https://fcp4lnexuap01.bocad.bank-banque-canada.ca/nexus/repository/pypi-all/"
pip config set global.index_url "https://fcp4lnexuap01.bocad.bank-banque-canada.ca/nexus/repository/pypi-all/simple"
 
run_command_with_retries "pip install azure-core" 3
run_command_with_retries "pip install azure-identity" 3
run_command_with_retries "pip install pandas" 3
run_command_with_retries 'pip install "oeds-common~=1.0"' 3

pip install openpyxl
