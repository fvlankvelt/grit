# rm -rf /tmp/graphdb-*

export RAFT_PEERS=1:localhost:8011,2:localhost:8012,3:localhost:8013

GRPC_PORT=8001 RAFT_PORT=8011 RAFT_ID=1 STORAGE_PATH=/tmp/graphdb-1 ./build/Grit > log-1 2>&1 &
peer_1_pid=$!
echo "Peer 1 running at 8001 - PID $peer_1_pid"

GRPC_PORT=8002 RAFT_PORT=8012 RAFT_ID=2 STORAGE_PATH=/tmp/graphdb-2 ./build/Grit > log-2 2>&1 &
peer_2_pid=$!
echo "Peer 2 running at 8002 - PID $peer_2_pid"

GRPC_PORT=8003 RAFT_PORT=8013 RAFT_ID=3 STORAGE_PATH=/tmp/graphdb-3 ./build/Grit > log-3 2>&1 &
peer_3_pid=$!
echo "Peer 3 running at 8003 - PID $peer_3_pid"

while true;
do
  sleep 10
done
