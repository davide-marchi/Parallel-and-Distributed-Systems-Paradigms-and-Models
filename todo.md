- Add march=native during compilation ?

OpenMP
- Bind threads to cores ? (Slide 45)
- Check if the shared array is neede (what if we dont specify it?)

Fastflow
- Run mapping_string.sh on remote machine (from inside the ff folder)
- simplify task struct (why atomic when we can have bool?)
- use init to make emitter generate the tasks and enqueue base cases asap