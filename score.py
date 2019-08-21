import argparse
import time
import json
import os
import requests
from models import create_tables, database, Bite
import logging
from logger import get_logger
import easysparql
from TPool.TPool import Pool as TPool
from PPool.Pool import Pool as PPool
from graph.type_graph import TypeGraph
from multiprocessing import Process, Lock, Pipe

tgraph = None
class_counts = dict()

UPLOAD_DIR = 'local_uploads'

logger = get_logger(__name__)
TESTING = False

MAX_NUM_PROCESSES = 5
MAX_NUM_OF_THREADS = MAX_NUM_PROCESSES


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
    # logger.debug("annotated: "+v)
    lock.acquire()
    pipe.send(d)
    lock.release()


def func_collect_annotations(pipe):
    cells = {}
    d = pipe.recv()
    while d is not None:
        logger.debug("collect: "+str(d))
        cells.update(d)
        d = pipe.recv()
    logger.info("Gotten the terminal signal")
    pipe.send(cells)
    while True:
        logger.info("waiting to get the data from the pipe")
        time.sleep(1)


def func_build_graph_hierarchy(class_uri, lock, endpoint):
    """
    This should be executed as a thread, and not as a process due to the shared global variable
    :param class_uri:
    :param lock:
    :param endpoint:
    :return:
    """
    global tgraph
    if tgraph.find_v(class_uri) is None:  # it is not in the graph
        lock.acquire()
        tgraph.add_v(class_uri, None)
        lock.release()
        parents = easysparql.get_parents_of_class(class_uri=class_uri, endpoint=endpoint)
        for p in parents:
            func_build_graph_hierarchy(p, lock, endpoint)
            lock.acquire()
            tgraph.add_e(p, class_uri)
            lock.release()


def build_graph(bite, endpoint):
    """
    :param bite:
    :param endpoint:
    :return:
    """
    global tgraph
    tgraph = TypeGraph()
    f = open(os.path.join(UPLOAD_DIR, bite.fname))
    j = json.load(f)
    classes = []
    for cell in j["data"].keys():
        for entity in j["data"][cell].keys():
            for class_uri in j["data"][cell][entity]:
                classes.append(class_uri)
    unique_classes = list(set(classes))
    params = []
    lock = Lock()
    for c in unique_classes:
        p = (c, lock, endpoint)
        params.append(p)
    logger.info("run thread pool")
    pool = TPool(max_num_of_threads=MAX_NUM_OF_THREADS, func=func_build_graph_hierarchy, params_list=params, logger=logger)
    pool.run()
    # to infer the roots automatically
    tgraph.build_roots()
    logger.info("thread pool is complete. The graph hierarchy should be ready")


def annotate_column(bite, endpoint, onlydomain):
    """
    :param bite:
    :param endpoint:
    :param onlydomain:

    :return: annotated cells (dict)
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
    pool = PPool(max_num_of_processes=MAX_NUM_PROCESSES, func=func_annotate_cell, params_list=params)
    pool.run()
    logger.info("sending signal to ask for the annotations")
    pipe_a.send(None)
    annotated_cells = pipe_a.recv()
    logger.info("annotated cells are recovered")
    collector_process.terminate()
    # logger.debug("annotated cells: "+str(annotated_cells))
    os.remove(fdir)
    json_fname = ".".join(bite.fname.split(".")[:-1])+".json"
    bite.fname = json_fname
    bite.save()
    json_str = json.dumps({"data": annotated_cells})
    f = open(os.path.join(UPLOAD_DIR, bite.fname), "w")
    f.write(json_str)
    f.close()


def compute_counts_of_a_class(class_uri, lock, endpoint):
    global class_counts

    lock.acquire()
    counted = class_uri in class_counts
    lock.release()
    if not counted:
        lock.acquire()
        class_counts[class_uri] = None
        lock.release()
        counts = easysparql.get_classes_subjects_count(classes=[class_uri], endpoint=endpoint)
        lock.acquire()
        class_counts[class_uri] = counts[class_uri]
        lock.release()


def compute_classes_counts(endpoint):
    global tgraph
    global class_counts
    params = []
    lock = Lock()
    logger.info("computer classes counts")
    for class_uri in tgraph.cache:
        p = (class_uri, lock, endpoint)
        params.append(p)
    pool = TPool(max_num_of_threads=MAX_NUM_OF_THREADS, func=compute_counts_of_a_class, params_list=params)
    pool.run()

    # The below two loops are to fix counts of classes that the endpoint could not return
    max_num = 0
    for k in class_counts:
        if class_counts[k] > max_num:
            max_num = class_counts[k]

    for k in class_counts:
        if class_counts[k] == -1:
            class_counts[k] = max_num * 2

    tgraph.set_nodes_subjects_counts(d=class_counts)


def compute_coverage_score_for_graph(bite):
    """
    :param bite:
    :return:
    """

    f = open(os.path.join(UPLOAD_DIR, bite.fname))
    j = json.load(f)
    classes = []
    m = 0
    for cell in j["data"].keys():
        if len(j["data"][cell].keys()) == 0:
            continue
        m += 1
        e_score = 1.0 / len(j["data"][cell].keys())
        d = {
        }
        for entity in j["data"][cell].keys():
            if len(j["data"][cell][entity]) == 0:
                c_score = 0
            else:
                c_score = 1.0 / len(j["data"][cell][entity])
            for class_uri in j["data"][cell][entity]:
                if class_uri not in d:
                    d[class_uri] = []
                d[class_uri].append(c_score*e_score)

        for curi in d.keys():
            curi_cov = sum(d[curi]) * 1.0 / len(d[curi])
            n = tgraph.find_v(curi)
            if n is None:
                print "couldn't find %s" % curi
            n.coverage_score += curi_cov
        del d
    tgraph.set_converage_score()
    return m


def compute_specificity_score_for_graph(endpoint):
    """
    :return:
    """
    global tgraph
    compute_classes_counts(endpoint=endpoint)
    logger.info("set instance specificity")
    tgraph.set_specificity_score()
    logger.info("set path specificity")
    tgraph.set_path_specificity()
    tgraph.set_depth_for_graph()


def graph_fname_from_bite(bite):
    graph_fname = "%d--%s--%d--%d.json" % (bite.id, bite.table, bite.column, bite.slice)
    graph_fname.replace(' ', '_')
    return graph_fname


def compute_scores(bite, endpoint):
    """
    :return: the dir of the graph file, m (the number of annotated cells)
    """
    m = compute_coverage_score_for_graph(bite=bite)
    compute_specificity_score_for_graph(endpoint=endpoint)
    graph_file_name = graph_fname_from_bite(bite)
    graph_file_dir = os.path.join(UPLOAD_DIR, graph_file_name)
    tgraph.save(graph_file_dir)
    # bite.fname = graph_file_name
    # bite.save()
    logger.info("graph_file_dir: "+graph_file_dir)
    return graph_file_dir, m


def get_m(file_dir):
    """
    :param file_dir:
    :return: m
    """
    f = open(file_dir)
    j = json.loads(f.read())
    m = 0
    for cell in j["data"].keys():
        if len(j["data"][cell].keys()):  # has at least one entity
            m+=1
    f.close()
    return m


def send_scored_graph(bite, graph_file_dir, m):
    """
    :param bite:
    :param graph_file_dir:
    :param m: The number of annotated cells
    :return:
    """
    f = open(graph_file_dir)
    content = f.read()
    files = {'graph': ("graph", content)}
    # m = get_m(os.path.join(UPLOAD_DIR, bite.fname))
    values = {'table': bite.table, 'column': bite.column, 'slice': bite.slice, 'total': bite.total,
              'm': m}
    logger.debug("bite address: ")
    logger.debug(bite.addr)
    logger.info("TESTING: "+str(TESTING))

    if not TESTING:
        url = bite.addr
        if url[-1] != "/":
            url += "/"
        url += "add"
        logger.info("URL str: ")
        logger.info(url)
        r = requests.post(url, files=files, data=values)
        if r.status_code == 200:
            logger.info("graph is sent to combine: "+str(bite.addr))
        else:
            logger.error("Error from combine: "+str(r.content))
    else:
        logger.info("testing is open and hence, the graph will not be sent to the combine")


def score(slice_id, endpoint, onlydomain):
    """
    # annotate each cell
    # build graph
    # compute scores
    # send scored graph to combine

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
        build_graph(bite=bite, endpoint=endpoint)
        graph_dir, m = compute_scores(bite=bite, endpoint=endpoint)
        logger.info("m = %d" % m)
        send_scored_graph(bite=bite, graph_file_dir=graph_dir, m=m)

    return True


def parse_args(args=None):
    global logger
    global TESTING
    parser = argparse.ArgumentParser(description='Score a given slice of data')
    parser.add_argument('--id', help="The id of the slice to be scored")
    parser.add_argument('--endpoint', default='http://dbpedia.org/sparql', help='The url of the SPARQL endpoint')
    parser.add_argument('--onlydomain', default='http://dbpedia.org/ontology/',
                        help="Restrict the annotation of cells to include the given domain")
    parser.add_argument('--testing', action="store_true", help="enable testing")
    parser.add_argument('--getm', help="get m from a given file")
    if args is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(args)

    if args.getm:
        print(get_m(args.getm))
        return

    if args.testing:
        logger = get_logger(__name__, logging.DEBUG)
        TESTING = True
        logger.info("Testing is enabled")

    if args.id:
        score(slice_id=args.id, endpoint=args.endpoint, onlydomain=args.onlydomain)
    else:
        parser.print_help()


if __name__ == '__main__':
    create_tables()
    database.connect()
    parse_args()
    database.close()

