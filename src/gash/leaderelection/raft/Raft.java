package gash.leaderelection.raft;

import gash.messaging.Message;
import gash.messaging.Message.Delivery;
import gash.messaging.Node;
import gash.messaging.transports.Bus;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Raft consensus algorithm is similar to PAXOS and Flood Max though it
 * claims to be easier to understand. The paper "In Search of an Understandable
 * Consensus Algorithm" explains the concept. See
 * https://ramcloud.stanford.edu/raft.pdf
 * 
 * Note the Raft algo is both a leader election and consensus algo. It ties the
 * election process to the state of its distributed log (state) as the state is
 * part of the decision process of which node should be the leader.
 * 
 * 
 * @author gash
 * 
 */
@SuppressWarnings({ "rawtypes", "unchecked","unused" })
public class Raft {
	static AtomicInteger msgID = new AtomicInteger(0);

	private Bus<? extends RaftMessage> transport;

	public Raft() {
		transport = new Bus<RaftMessage>(0);
	}

	
	public void addNode( RaftNode node) {
		if (node == null)
			return;

		node.setTransport(transport);

		
		Node<Message> n = (Node) (node);
		transport.addNode(n);

		if (!node.isAlive())
			node.start();
	}

	/** processes heartbeats */
	public interface HeartMonitorListener {
		public void doMonitor();
	}

	public static abstract class LogEntryBase {
		private int term;
	}

	
	private static class LogEntry extends LogEntryBase {

	}

	/** triggers monitoring of the heartbeat */
	public static class RaftMonitor extends TimerTask {
		private RaftNode<RaftMessage> node;

		public RaftMonitor(RaftNode<RaftMessage> node) {
			if (node == null)
				throw new RuntimeException("Missing node");

			this.node = node;
		}

		@Override
		public void run() {
			node.checkBeats();
		}
	}

	/** our network node */
	public static class RaftNode<M extends RaftMessage> extends Node<M> {
		public enum RState {
			Follower, Candidate, Leader
		}

		/* Added by Jvalant on 8th March starts */
		private int leaderID = -1;
		private RState currentState = RState.Follower;
		private long lastKnownBeat = System.currentTimeMillis();
		private int beatSensitivity = 3; // Threshold value for timeout
		private int beatDelta = 3000; // timeout value, it should be random.
		private int beatCounter = 0; // when beatCounter>= beatSensitivity,
										// leaderNotice is sent by node.
		private Timer timer;
		private RaftMonitor monitor;
		private int voteCounter;
		private int maxTerm = 0;
		private int leaderTTL = 0; //for testing - to begin the election after some time. 

		/* Added by Jvalant on 8th March ends */

		private Bus<? extends RaftMessage> transport;

		public RaftNode(int id) {
			super(id);

			// added by Jvalant on 8th March starts
			Random rand = new Random();
			int value = rand.nextInt(10000);
			if (value < 5000)
				value = value + 3000;
			this.beatDelta = value;
			System.out.println("beatDelta for node "+id+"is "+this.beatDelta);
			// added by Jvalant on 8th March ends
		}

		/* Added by Jvalant on 8th March starts */
		public RaftNode(int ID, int beatSensitivity, int beatDelta) {
			super(ID);
			this.beatSensitivity = beatSensitivity;
			this.beatDelta = beatDelta;
		}

		public void start() {
			if (this.timer != null)
				return;

			monitor = new RaftMonitor((RaftNode<RaftMessage>) this);

			// allow the threads to start before checking HB. Also, the
			// beatDelta should be slightly greater than the frequency in which
			// messages are emitted to reduce false positives.
			int freq = (int) (beatDelta * .75);
			if (freq == 0)
				freq = 1;
			System.out.println("Frequency for the Monitor of Node "+getNodeId()+" is set to "+freq);
			timer = new Timer();
			timer.scheduleAtFixedRate(monitor, beatDelta * 2, freq);

			super.start();
		}

		protected void checkBeats() {
			System.out.println("--> node " + getNodeId()
					+ " heartbeat (counter = " + beatCounter + ")");

			// leader must sent HB to other nodes otherwise an election will
			// start
			if (this.leaderID == getNodeId()) {
				//leaderID = -1;
				beatCounter = 0;
				if(leaderTTL<=3){
				sendAppendNotice();   //check the heartbeats of nodes
				leaderTTL++;
				}
				return;
			} else if (currentState.equals(RaftNode.RState.Candidate)) {
				// ignore triggers from HB as we are in an election
				beatCounter = 0;
				return;
			}

			long now = System.currentTimeMillis();
			if (now - lastKnownBeat > beatDelta
					&& (currentState != RaftNode.RState.Candidate)) {
				beatCounter++;
				if (beatCounter > beatSensitivity) {
					System.out.println("--> node " + getNodeId()
							+ " starting an election");
					// leader is dead! Long live me!
					currentState = RState.Candidate;
					sendRequestVoteNotice();
				}
			}
		}

		private void processReceivedMessage(RaftMessage msg) {
			
			switch (msg.getAction()) {
			
			case RequestVote:
							if (this.currentState.equals(RState.Candidate)) {
								if (msg.getOriginator() == getNodeId() /*&& leaderID == -1*/) {
			
									System.out.println("Node " + getNodeId()
											+ "gives vote to itself....");
									this.voteCounter++;
									if (voteCounter > this.transport.getNodes().length / 2){
										sendLeaderNotice();
										this.leaderID = getNodeId();
										voteCounter = 0;
										leaderTTL = 0;
										currentState = RaftNode.RState.Leader;
										}
								} else if (msg.getOriginator() != getNodeId()) {
									// I'm a better candidate
									System.out
											.println("--> node "
													+ getNodeId()
													+ " is in candidate state, so ignoring request vote from  "
													+ msg.getOriginator());
			
								}
							} else if (this.currentState.equals(RState.Leader)) {
			
							} else {
								System.out.println("Node "+getNodeId()+" sending Vote to Node "+msg.getOriginator());
								sendVoteNotice(msg.getOriginator());
							}
							break;
							
			case Vote:	
						if(this.currentState.equals(RState.Candidate)){
							this.voteCounter++;
							System.out.println("Node "+getNodeId()+" received Vote from Node "+msg.getOriginator());
							if (voteCounter > this.transport.getNodes().length / 2){
								sendLeaderNotice();
								this.leaderID = getNodeId();
								voteCounter = 0;
								currentState = RaftNode.RState.Leader;
								leaderTTL = 0;
								System.out.println("Node "+getNodeId()+" declares itself as a Leader..");
							}
						}
						break;
						
			case Leader:					
						System.out.println("--> node " + getNodeId()
								+ " acknowledges the leader is " + msg.getOriginator());
		
						// things to do when we get a HB
						leaderID = msg.getOriginator();
						lastKnownBeat = System.currentTimeMillis();
						currentState = RaftNode.RState.Follower;
						beatCounter = 0;
						break;
						
			case Append:
						if(this.currentState==RaftNode.RState.Follower){  //heartbeat check from Leader
							System.out.println("Heartbeat check for Node "+getNodeId()+" from Leader Node "+msg.getOriginator());
							//leaderID = msg.getOriginator();
							lastKnownBeat = System.currentTimeMillis();
							currentState = RaftNode.RState.Follower;
							beatCounter = 0;
							//sendAppendNotice(msg.getOriginator());  // reply to Leader
						}
						else if(this.currentState.equals(RaftNode.RState.Leader)){
							//Acknowledgment from the follower node 
							System.out.println("Acknowledgment from the follower node "+msg.getOriginator()+" for Leader Node "+getNodeId());
						}
						break;
						
			default:
						break;
			}
			
		}

		/* Added by Jvalant on 8th March ends */

		private void sendLeaderNotice() {
			RaftMessage msg = new RaftMessage(Raft.msgID.incrementAndGet());
			msg.setOriginator(getNodeId());
			msg.setDeliverAs(Delivery.Broadcast);
			msg.setDestination(-1);
			msg.setAction(RaftMessage.Action.Leader);
			send(msg);
		}

		private void sendAppendNotice() {
			RaftMessage msg = new RaftMessage(Raft.msgID.incrementAndGet());
			msg.setOriginator(getNodeId());
			msg.setDeliverAs(Delivery.Broadcast);
			msg.setDestination(-1);
			msg.setAction(RaftMessage.Action.Append);
			send(msg);
		}
		
		private void sendAppendNotice(int NodeId) {
			RaftMessage msg = new RaftMessage(Raft.msgID.incrementAndGet());
			msg.setOriginator(getNodeId());
			msg.setDeliverAs(Delivery.Direct);
			msg.setDestination(NodeId);
			msg.setAction(RaftMessage.Action.Append);
			send(msg);
		}

		/** TODO args should set voting preference */
		private void sendRequestVoteNotice() {
			RaftMessage msg = new RaftMessage(Raft.msgID.incrementAndGet());
			msg.setOriginator(getNodeId());
			msg.setDeliverAs(Delivery.Broadcast);
			msg.setDestination(-1);
			msg.setAction(RaftMessage.Action.RequestVote);
			send(msg);
		}

		/* Message type added by Jvalant on 9th March starts */
		private void sendVoteNotice(int nodeId) {
			RaftMessage msg = new RaftMessage(Raft.msgID.incrementAndGet());
			msg.setOriginator(getNodeId());
			msg.setDeliverAs(Delivery.Direct);
			msg.setDestination(nodeId);
			msg.setAction(RaftMessage.Action.Vote);
			send(msg);
		}
		
		/* Message type added by Jvalant on 9th March ends */

		private void send(RaftMessage msg) {
			// enqueue the message - if we directly call the nodes method, we
			// end up with a deep call stack and not a message-based model.
			transport.sendMessage(msg);
		}

		/** this is called by the Node's run() - reads from its inbox */
		@Override
		public void process(RaftMessage msg) {
			processReceivedMessage((RaftMessage) msg);
		}

		public void setTransport(Bus<? extends RaftMessage> t) {
			this.transport = t;
		}
	}
}
