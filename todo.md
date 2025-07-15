- Add march=native during compilation ?

- Fix the passing of the -c flag!
    while ((c = getopt_long(argc, argv, "n:p:t:qc h", long_opts, nullptr)) != -1) {


OpenMP
- Bind threads to cores ? (Slide 45)
- Check if the shared array is neede (what if we dont specify it?)

Fastflow
- Run mapping_string.sh on remote machine (from inside the ff folder)
- tweak scheduling and num threads
- simplify task struct
    - remove is_sort! sorting condition can be calculated by the worker (offset could be worker private variable)
    - why atomic when we can have bool?
    - optimize build_task
        - operations inside contructor (inline)
        - no return value
    - instead of left, mid and right can we have like pointer fo first element, to the mid one and total lenght?
        - would remove global variable but would reuire to change the merge_record()
- use init to make emitter generate the tasks and enqueue base cases asap
    -WATCH OUT for the garbage collector (but shouldnt be a problem)