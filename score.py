import argparse
import time
import json
import os
from models import create_tables, database, Bite
from logger import get_logger
import easysparql
from PPool.Pool import Pool
from graph.type_graph import TypeGraph
from multiprocessing import Process, Lock, Pipe


UPLOAD_DIR = 'local_uploads'

logger = get_logger(__name__)


MAX_NUM_PROCESSES = 5


def annotate_cell(v, endpoint, onlydomain):
    """
    :param v: cell value
    :param endpoint:
    :param onlydomain:
    :return: cell_value: {
                        entity1: [classes]
                        entity2: [classes]
                        ....
                        }
    """
    d = dict()
    entities = easysparql.get_entities(subject_name=v, endpoint=endpoint)
    for e in entities:
        d[e] = []
        classes = easysparql.get_classes(entity=e, endpoint=endpoint)
        for c in classes:
            if onlydomain is None or c.startswith(onlydomain):
                d[e].append(c)
        if d[e] == []:
            del d[e]
    return {
        v: d
    }


def func_annotate_cell(v, endpoint, onlydomain, lock, pipe):
    """
    :param v:
    :param endpoint:
    :param onlydomain:
    :param lock:
    :param pipe:
    :return:
    """
    logger.debug("in func_annotate_cell")
    d = annotate_cell(v, endpoint, onlydomain)
    logger.debug("annotated: "+v)
    lock.acquire()
    pipe.send(d)
    lock.release()


def func_collect_annotations(pipe):
    cells = []
    d = pipe.recv()
    while d is not None:
        logger.debug("collect: "+str(d))
        cells.append(d)
        d = pipe.recv()
    logger.debug("Gotten the terminal signal")
    pipe.send(cells)
    while True:
        logger.debug("waiting to get the data from the pipe")
        time.sleep(1)


def workflow():
    # annotate each cell
    # build graph
    # compute scores
    # send scored graph to combine
    pass


def annotate_column(bite, endpoint, onlydomain):
    """
    :param fname:
    :return:
    """
    fdir = os.path.join(UPLOAD_DIR, bite.fname)
    f = open(fdir)
    values = f.read().split('\t')
    lock = Lock()
    pipe_a, pipe_b = Pipe(True)  # duplex
    params = []
    for v in values:
        p = (v, endpoint, onlydomain, lock, pipe_a)
        params.append(p)
    collector_process = Process(target=func_collect_annotations, args=(pipe_b,))
    collector_process.start()
    pool = Pool(max_num_of_processes=MAX_NUM_PROCESSES, func=func_annotate_cell, params_list=params)
    pool.run()
    logger.debug("sending signal to ask for the annotations")
    pipe_a.send(None)
    annotated_cells = pipe_a.recv()
    logger.debug("annotated cells are recovered")
    collector_process.terminate()
    logger.debug("annotated cells: "+str(annotated_cells))
    os.remove(fdir)
    bite.fname = ""
    bite.save()
    json_str = json.dumps({"data": annotated_cells})
    f = open(os.path.join(UPLOAD_DIR, "data.json"), "w")
    f.write(json_str)
    f.close()


def score(slice_id, endpoint, onlydomain):
    """

    :param slice_id:
    :param endpoint:
    :param onlydomain:
    :return:
    """

    bites = Bite.select().where(Bite.id==slice_id)
    if len(bites) == 0:
        logger.warning("No bite with id: %s" % slice_id)
        return False
    else:
        bite = bites[0]
        logger.debug("The bite is found")
        annotate_column(bite, endpoint, onlydomain)
    #
    #
    # if b.addr != '':
    #     logger.debug("\nsending to combine: " + str(get_params))
    #     r = requests.get(b.addr+"/add", params=get_params)
    #     if r.status_code != 200:
    #         logger.debug("error: "+r.content)
    #         return jsonify({'error': 'Error received from the combine instance: '+r.content}, status_code=500)
    # else:
    #     logger.debug("\nempty address of combine service: " + str(get_params))
    # return jsonify({'msg': 'data received and processed'})


def parse_args(args=None):
    parser = argparse.ArgumentParser(description='Score a given slice of data')
    parser.add_argument('--id', help="The id of the slice to be scored")
    parser.add_argument('--endpoint', default='https://dbpedia.org/sparql', help='The url of the SPARQL endpoint')
    parser.add_argument('--onlydomain', default='http://dbpedia.org/ontology/', help="Restrict the annotation of cells to include the given domain")
    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    if args.id:
        score(slice_id=args.id, endpoint=args.endpoint, onlydomain=args.onlydomain)
    else:
        parser.print_help()


if __name__ == '__main__':
    create_tables()
    database.connect()
    parse_args()
    database.close()

