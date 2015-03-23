package gash.leaderelection;

import gash.leaderelection.raft.Raft;
import gash.leaderelection.raft.Raft.RaftNode;
import gash.leaderelection.raft.RaftMessage;

import org.junit.Test;

public class RaftTest {

	@Test
	public void test() throws InterruptedException {
		
		Raft raft = new Raft();
		
		RaftNode<RaftMessage> node1 = new RaftNode<RaftMessage>(1);
		RaftNode<RaftMessage> node2 = new RaftNode<RaftMessage>(2);
		RaftNode<RaftMessage> node3 = new RaftNode<RaftMessage>(3);
		
		raft.addNode(node1);
		raft.addNode(node2);
		raft.addNode(node3);
		
		
		Thread.sleep(200000);
		node1.destroy();
		node2.destroy();
		node3.destroy();
		
	}

}
