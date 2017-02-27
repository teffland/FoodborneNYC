
# coding: utf-8

# In[1]:

import argparse
import logging
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-8s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

import json
import numpy as np
import datetime as dt

import spacy

import foodbornenyc.util.util as u

def flatten(INPUT, TEXT_PATH, LOGGING_FREQ):
    logging.info( 'reading reviews jsons from file, '+INPUT)
    logging.info( 'writing flattened reviews to file, '+TEXT_PATH )
    with open(INPUT) as f, open(TEXT_PATH, 'w') as f2:
        c1 = 0
        c2 = 0
        for l in f:
            j = json.loads(l)
            reviews = j.get('reviews', [])
            for r in reviews:
                text = u.xuni( r['text'] )
                try:
                    f2.write(" ".join(text.split("\n")) +"\n")
                except Exception as e:
                    logger.exception( e )
                    logger.info( text )
                c2+=1
            c1+=1
            if( 0 == c1%LOGGING_FREQ ):
                logger.info('{} businesses, {} reviews processed'.format(c1, c2))
            # end reviews
        # end lines
        logger.info('{} businesses, {} reviews processed'.format(c1, c2))
    # end file

def tokenize(INPUT, OUTPUT, LOGGING_FREQ):
    logging.info( 'loading spaCy module')
    sp = spacy.load('en')
    logging.info( 'reading flattened reviews from file, '+INPUT)
    logging.info( 'writing preprocessed reviews to file, '+OUTPUT)
    c1 = 0
    with open(INPUT, 'r') as infile, open(OUTPUT, 'w') as outfile:
        for line in infile:
            tokens = [x.text for x in sp(u.xuni(line)) if x.pos_!='PUNCT']
            line2 = u.xuni(" ".join(tokens))
            outfile.write(line2)
            c1+=1
            if( 0 == c1%LOGGING_FREQ ):
                logger.info('{} lines processed'.format(c1))
        # end line
    # close infile outfile
    return

def test(args):
    s = 'running with arguments:\n'
    for k,v in args.items():
        s+=str(k)+': '+str(v)+'\n'
    logger.info(s)
    return

DATA_DIR='/tmp/yo/'

def parseArgs():
    p = argparse.ArgumentParser()
    p.add_argument('command', type=str)
    p.add_argument('-m', '--mode', default='demo', type=str)
    p.add_argument('-t', '--timestamp', default=dt.datetime.now().strftime("%Y%m%d_%H%M%S"), type=str)
    p.add_argument('-i', '--input', default=None, type=str)
    p.add_argument('-o', '--output', default=None, type=str)
    # p.add_argument('-i', '--input', default='./foodbornenyc/sources/yelpfiles/yelp_mini_sample.json', type=str)
    # p.add_argument('-i', '--input', default='./foodbornenyc/sources/yelpfiles/yelp_businesses.json', type=str)
    # p.add_argument('-o', '--output', default=None, type=str)
    p.add_argument('-l', '--logging_freq', default=1000, type=int)

    args = p.parse_args()
    dependent_args = {}
    if args.command == 'flatten':
        if args.mode == 'demo':
            dependent_args['input'] = DATA_DIR+'/yelp_mini_sample.json'
            dependent_args['output'] = DATA_DIR+'/yelp_text_sample_'+args.timestamp+'.txt'
        elif args.mode == 'prod':
            dependent_args['input'] = DATA_DIR+'/yelp_businesses.json'
            dependent_args['output'] = DATA_DIR+'/yelp_text_'+args.timestamp+'.txt'
    if args.command == 'tokenize':
        if args.mode == 'demo':
            dependent_args['input'] = DATA_DIR+'/yelp_text_sample.txt'
            dependent_args['output'] = DATA_DIR+'/yelp_preprocessed_sample_'+args.timestamp+'.txt'
        elif args.mode == 'prod':
            dependent_args['input'] = DATA_DIR+'/yelp_text.txt'
            dependent_args['output'] = DATA_DIR+'/yelp_preprocessed_'+args.timestamp+'.txt'

    args = {k:v for k,v in args.__dict__.items() if not v is None}
    dependent_args.update(args)
    return dependent_args

def main(args):
    assert type(args) is dict, type(args)

    INPUT    = args['input']
    TEXT_PATH    = args['output']
    LOGGING_FREQ = args['logging_freq']

    if(args['command'] == 'flatten'):
        test(args)
        flatten(INPUT, TEXT_PATH, LOGGING_FREQ)
    if(args['command'] == 'tokenize'):
        test(args)
        tokenize(INPUT, TEXT_PATH, LOGGING_FREQ)
    if(args['command'] == 'test'):
        test(args)

if __name__ == '__main__':
    args = parseArgs()
    main(args)
