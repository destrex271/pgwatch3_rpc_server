
#!/bin/bash

docker rm ollama || true

# start new ollama container
docker run -d -v ollama:/root/.ollama -p 11434:11434 --name ollama ollama/ollama
