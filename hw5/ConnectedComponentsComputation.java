import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Implementation of the connected component algorithm that identifies
 * connected components and assigns each vertex its "component
 * identifier" (the smallest vertex id in the component).
 */
public class ConnectedComponentsComputation extends
    BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {
    /**
    * Propagates the smallest vertex id to all neighbors. Will always choose to
    * halt and only reactivate if a smaller id has been sent to it.
    *
    * @param vertex Vertex
    * @param messages Iterator of messages from the previous superstep.
    * @throws IOException
    */
    @Override
    public void compute(
        Vertex<IntWritable, IntWritable, NullWritable> vertex,
        Iterable<IntWritable> messages) throws IOException{
        
        int self_id = vertex.getValue().get();
        
        if (getSuperstep() == 0){
            // Get new id from neighbors
            for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()){
                IntWritable neighbor = edge.getTargetVertexId();
                self_id = Math.min(self_id, neighbor.get());
            }
            
            // Broadcast 
            if (self_id != vertex.getValue().get()){
                vertex.setValue(new IntWritable(self_id));
                
                for (Edge<IntWritable, NullWritable> edge : vertex.getEdges()){
                    IntWritable neighbor = edge.getTargetVertexId();
                    if (neighbor.get() > self_id){
                        sendMessage(neighbor, vertex.getValue());
                    }
                }
            }
        }
        else{
            // Get new id from messages
            for (IntWritable msg : messages){
                self_id = Math.min(self_id, msg.get());
            }

            // Broadcast
            if (self_id != vertex.getValue().get()){
                vertex.setValue(new IntWritable(self_id));
                sendMessageToAllEdges(vertex, vertex.getValue());
            }
        }
        vertex.voteToHalt();
    }
}
