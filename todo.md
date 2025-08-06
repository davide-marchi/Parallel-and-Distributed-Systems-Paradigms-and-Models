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
    - instead of left, mid and right can we have like pointer fo first element, to the mid one and total lenght?
        - would remove global variable but would reuire to change the merge_record()


----

- Devo cancellare i file ordinati dopo il controllo?

- Usare posix_fallocate? usare madvise(map, exact_size, MADV_SEQUENTIAL);?

----

Option bottom-upp (per svrapporre generazione index e ordinamento):

base-case = 10'000

1. leggo il file

2. Ogni struct letta controllo:

    (Sono degli if all'interno di un for, NON uno switch!)

    - Se la struct è la numero 2^0 * base case: Ordino gli ultimi base-case con sort()

    - Se la struct è la numero 2^1 * base case: Mergio gli ultimi 2 array di dim. 2^0 * base-case

    - Se la struct è la numero 2^2 * base case: Mergio glu ultimi 2 array di dim. 2^1 * base case

    - ...

    Caso in cui la struct letta è l'ultima?