# Esto calcula el valor de Pi usando el algoritmo de la "diana". Este es un método de Monte Carlo
# que utiliza muchas pruebas para estimar el área de un cuarto de círculo dentro de una unidad cuadrada.
#
# Este código utiliza Dispy on OctaPi usando el método recomendado para manejar 
# los trabajos de manera eficiente. Para más información, visite la página web de Dispy. 
#
# Referencia: Arndt & Haenel, "Pi Uneashed", Springer-Verlag, 
# ISBN 978-3-540-66572-4, 2006, 

# Dispy:
# Giridhar Pemmasani, "dispy": Computación distribuida y paralela con/para Python",
# http://dispy.sourceforge.net, 2016

# Todos los demás códigos originales: Crown Copyright 2016, 2017 

# El "cómputo" se distribuye a cada nodo que ejecuta el "disipnodo".
def compute(s, n):
    import time, random

    inside = 0

    # establecer la semilla aleatoria en el servidor a partir de la que pasó el cliente
    random.seed(s)

    # para todos los puntos solicitados
    for i in range(n):
        # calcular la posición del punto
        x = random.uniform(0.0, 1.0)
        y = random.uniform(0.0, 1.0)
        z = x*x + y*y
        if (z<=1.0):
            inside = inside + 1    # este punto está dentro del círculo de la unidad

    return(s, inside)

# dispy llama a esta función para indicar el cambio en el estado del trabajo
def job_callback(job): # ejecutado en el cliente
    global pending_jobs, jobs_cond
    global total_inside

    if (job.status == dispy.DispyJob.Finished  # el caso más común
        or job.status in (dispy.DispyJob.Terminated, dispy.DispyJob.Cancelled,
                          dispy.DispyJob.Abandoned)):
        # 'pending_jobs' se comparte entre dos hilos, por lo que acceder a él con
        # 'jobs_cond' (ver abajo)
        jobs_cond.acquire()
        if job.id: # el trabajo puede haber terminado antes de la identificación asignada 'principal'.
            pending_jobs.pop(job.id)
            if (job.id % 1000 == 0):
                dispy.logger.info('job "%s" returned %s, %s jobs pending', job.id, job.result, len(pending_jobs))

            # extraer los resultados de cada trabajo a medida que sucede
            ran_seed, inside = job.result # devuelve los resultados del trabajo
            total_inside += inside        # contar el número de puntos dentro de un cuarto de círculo

            if len(pending_jobs) <= lower_bound:
                jobs_cond.notify()
        jobs_cond.release()

# main 
if __name__ == '__main__':
    import dispy, random, argparse, resource, threading, logging, decimal, socket

    # establecer límites inferiores y superiores según corresponda
    # El límite inferior es al menos un número de cpus y el límite superior es aproximadamente 3 veces el límite inferior.
    # lower_bound, upper_bound = 352, 1056
    lower_bound, upper_bound = 32, 96

    resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY) )
    resource.setrlimit(resource.RLIMIT_DATA, (resource.RLIM_INFINITY, resource.RLIM_INFINITY) )

    parser = argparse.ArgumentParser()
    parser.add_argument("no_of_points", type=int, help="number of random points to include in each job")
    parser.add_argument("no_of_jobs", type =int, help="number of jobs to run")
    args = parser.parse_args()

    no_of_points = args.no_of_points
    no_of_jobs = args.no_of_jobs
    server_nodes ='192.168.1.*'

    # usar la variable Condition para proteger el acceso a pending_jobs, como
    # 'job_callback' es ejecutado en otro hilo.
    jobs_cond = threading.Condition()

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    cluster = dispy.JobCluster(compute, ip_addr=s.getsockname()[0], nodes=server_nodes, callback=job_callback, loglevel=logging.INFO)
    #cluster = dispy.JobCluster(compute, nodes=server_nodes, callback=job_callback, loglevel=logging.INFO)
    pending_jobs = {}

    print(('submitting %i jobs of %i points each to %s' % (no_of_jobs, no_of_points, server_nodes)))
    total_inside = 0
    i = 0
    while i <= no_of_jobs:
        i += 1

        # programar la ejecución de "computación" en un nodo (ejecutando "dispynode")
        ran_seed = random.randint(0,65535) # definir una semilla al azar para cada servidor usando el cliente RNG
        job = cluster.submit(ran_seed, no_of_points)

        jobs_cond.acquire()

        job.id = i # asociar una identificación al trabajo

        # hay una posibilidad de que el trabajo haya terminado y se haya llamado 
        # a job_callback en este momento, así que ponlo en 'pending_jobs' sólo si el trabajo está pendiente
        if job.status == dispy.DispyJob.Created or job.status == dispy.DispyJob.Running:
            pending_jobs[i] = job
            # dispy.logger.info('job "%s" submitted: %s', i, len(pending_jobs))
            if len(pending_jobs) >= upper_bound:
                while len(pending_jobs) > lower_bound:
                    jobs_cond.wait()
        jobs_cond.release()

    cluster.wait()

    # calcular el valor estimado de Pi
    total_no_of_points = no_of_points * no_of_jobs
    decimal.getcontext().prec = 100  # anular la precisión estándar
    Pi = decimal.Decimal(4 * total_inside / total_no_of_points)
    print(('value of Pi is estimated to be %s using %i points' % (Pi, total_no_of_points) ))

    cluster.print_status()
    cluster.close()
