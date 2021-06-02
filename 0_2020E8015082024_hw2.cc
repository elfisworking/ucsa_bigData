/**
 * @file PageRankVertex.cc
 * @author  Songjie Niu, Shimin Chen
 * @version 0.1
 *
 * @section LICENSE 
 * 
 * Copyright 2016 Shimin Chen (chensm@ict.ac.cn) and
 * Songjie Niu (niusongjie@ict.ac.cn)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * @section DESCRIPTION
 * 
 * This file implements the PageRank algorithm using graphlite API.
 *
 */

#include <stdio.h>
#include <string.h>
#include <math.h>
#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) SSSP##name

#define EPS 1e-6

int v0_id;

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(double);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(double);
        return m_m_value_size;
    }
    void loadGraph() {
        if (m_total_edge <= 0)  return;

        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        double MAX_INFI = 0x7fffffff;
        double value = 1;
        // represnt max number
        int outdegree = 0;
        
        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable
        //read 0 edge
        sscanf(line, "%lld %lld %lf", &from, &to,&weight);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        //read from 1 to m_total_edge
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld %lf", &from, &to,&weight);
            if (last_vertex != from) {
                // if(last_vertex == (unsigned long long)v0_id){
                //     printf("%lld %d",last_vertex,v0_id);
                    addVertex(last_vertex,&value,outdegree);
                // }else{
                //     addVertex(last_vertex, &MAX_INFI, outdegree);
                // }
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        // if(last_vertex == (unsigned long long)v0_id){
            addVertex(last_vertex,&value,outdegree);
        // }else{
        //     addVertex(last_vertex, &MAX_INFI, outdegree);
        // }
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    //not need change. This method fit requirement.
    void writeResult() {
        int64_t vid;
        double value;
        char s[1024];

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "%lld: %f\n", (unsigned long long)vid, value);
            writeNextResLine(s, n);
        }
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<double> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (double *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        m_global += * (double *)p;
    }
    void accumulate(const void* p) {
        m_local += * (double *)p;
    }
};

class VERTEX_CLASS_NAME(): public Vertex <double, double, double> {
public:
    void compute(MessageIterator* pmsgs) {
        // judge whether finish or not 
        if(getSuperstep() >= 2){
        double global_change_number = *(double *)getAggrGlobal(0);
        if(global_change_number==0){
            voteToHalt();
            return ;
            }
        }
        //when up this steps also stop
        if (getSuperstep() >= 110){
			voteToHalt();
			return;
		}
        // change value when superstep is 0
        if(getSuperstep() == 0){
            if((double)getVertexId() != v0_id){
                *mutableValue() = 0x7fffffff;
            }	
	        else{
                *mutableValue() = 0;
            }
        }

        // get current min distance 
        double min_distance = getValue();
        double temp_value = min_distance;
        //reveice message
        for(;!pmsgs->done();pmsgs->next()){
            min_distance = fmin(pmsgs->getValue(),min_distance);
        }
            // update min distance 
        if(temp_value!=min_distance){
            * mutableValue() = min_distance;
            double one = 1; 
            accumulateAggr(0,&one);
        }
            // send message to each neighbour
            OutEdgeIterator iterator = getOutEdgeIterator();
            for(;!iterator.done();iterator.next()){
                sendMessageTo(iterator.target(),min_distance+iterator.getValue());
            }
        

    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);
        // modify below change 3 to 4 for adding weight 
        if (argc < 3) {
           printf ("Usage: %s <input path> <output path>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        v0_id = atoi(argv[3]);

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}