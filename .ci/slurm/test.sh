BRANCH_NAME=$1

# The base Slurm image does not ship canary or hpc-connect; install the
# branch under test at runtime so a single pre-built image can be reused by
# every CI run.
python3 -m venv /canary
source /canary/bin/activate
python3 -m pip install --upgrade pip==25.1.1
python3 -m pip install "canary-wm@git+https://git@github.com/sandialabs/canary"
python3 -m pip install "hpc-connect@git+https://git@github.com/sandialabs/hpc-connect@$BRANCH_NAME"

canary fetch examples

echo " "
echo " "
echo " "
echo " "
echo "----------------------------------------------------"
echo "Starting Slurm tests..."
echo " "
echo " "
echo " "

echo "------------------------Test 1----------------------"
echo " "
# Test 1
exit_code=0
canary -d run --show-excluded-tests -w -b scheduler=slurm -b spec=count:3,nodes:any ./examples || exit_code=$?
if [ "${exit_code}" -ne 14 ]; then
  cat .canary/cache/canary-hpc/batches/*/resource_pool.json || true
  cat .canary/cache/canary-hpc/batches/*/canary-out.txt || true
  cat TestResults/basic/second/second/canary-out.txt || true
  cat TestResults/basic/second/second/canary-err.txt || true
  exit 1
fi

echo " "
echo " "
echo "----------------------- Done! ----------------------"
