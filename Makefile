build_grpc_files: install_requirements
	python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. protos/task_queue.proto

install_requirements:
	pip install -r requirements.txt

clean:
	rm -rf __pycache__
	rm -rf */__pycache__
	rm -f protos/task_queue_pb2.py protos/task_queue_pb2_grpc.py