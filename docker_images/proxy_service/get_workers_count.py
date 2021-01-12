# https://docs.gunicorn.org/en/stable/design.html#how-many-workers
#
# >Generally we recommend (2 x $num_cores) + 1 as the number of workers to start off with.
import multiprocessing

print((2 * multiprocessing.cpu_count()) + 1)
