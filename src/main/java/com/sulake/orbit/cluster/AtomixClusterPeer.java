package com.sulake.orbit.cluster;

import cloud.orbit.actors.cluster.ClusterPeer;
import cloud.orbit.actors.cluster.MessageListener;
import cloud.orbit.actors.cluster.NodeAddress;
import cloud.orbit.actors.cluster.NodeAddressImpl;
import cloud.orbit.actors.cluster.ViewListener;
import cloud.orbit.concurrent.Task;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.core.Atomix;
import io.atomix.messaging.Endpoint;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import static java.util.UUID.nameUUIDFromBytes;

/**
 * @author Johno Crawford (johno@sulake.com)
 */
public class AtomixClusterPeer implements ClusterPeer {

    private static final Logger logger = LogManager.getLogger(AtomixClusterPeer.class);

    private static final String DIRECT_MESSAGING = "direct-messaging";

    private final Node[] bootstrapNodes;
    private final int port;
    private final Executor executor;

    private final ConcurrentHashMap<String, AtomixConcurrentMap> caches = new ConcurrentHashMap<>();

    private final static Serializer CLUSTER_PEER_SERIALIZER = Serializer.using(KryoNamespace.builder()
            .setRegistrationRequired(false)
            .register(KryoNamespaces.BASIC)
            .register(Payload.class)
            .build());

    private Atomix atomix;

    private NodeAddress localNodeAddress;
    private final Map<NodeAddress, NodeId> nodeMap = new ConcurrentHashMap<>();

    private ViewListener viewListener;
    private MessageListener messageListener;

    public AtomixClusterPeer(Node[] bootstrapNodes, int port, Executor executor) {
        this.bootstrapNodes = bootstrapNodes;
        this.port = port;
        this.executor = executor;
    }

    @Override
    public NodeAddress localAddress() {
        return localNodeAddress;
    }

    @Override
    public void registerViewListener(final ViewListener viewListener) {
        this.viewListener = viewListener;
    }

    @Override
    public void registerMessageReceiver(final MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    @Override
    public void sendMessage(NodeAddress address, byte[] bytes) {
        final NodeId node = nodeMap.get(address);
        if (node == null) {
            throw new IllegalArgumentException("Cluster node not found: " + address);
        }
        atomix.messagingService().unicast(DIRECT_MESSAGING, new Payload(localNodeAddress, bytes),
                CLUSTER_PEER_SERIALIZER::encode, node);
    }

    private static final class Payload implements Serializable {
        private static final long serialVersionUID = 1L;

        private final NodeAddress nodeAddress;
        private final byte[] bytes;

        Payload(NodeAddress nodeAddress, byte[] bytes) {
            this.nodeAddress = nodeAddress;
            this.bytes = bytes;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> ConcurrentMap<K, V> getCache(String cacheName) {
        return getAtomixCache(cacheName);
    }

    @SuppressWarnings("unchecked")
    public <K, V> AtomixConcurrentMap<K, V> getAtomixCache(String cacheName) {
        final AtomixConcurrentMap<K, V> cache = caches.get(cacheName);
        if (cache != null) {
            return cache;
        }
        return caches.computeIfAbsent(cacheName, s -> new AtomixConcurrentMap(atomix, s));
    }

    @Override
    public Task<?> join(final String clusterName, final String nodeName) {
        return Task.from(createAtomicNode(nodeName, port, bootstrapNodes).start().thenAccept((atomix) -> {
            this.atomix = atomix;
            this.localNodeAddress = new NodeAddressImpl(nameUUIDFromBytes(atomix.clusterService()
                    .getLocalNode().id().id().getBytes()));
            atomix.messagingService().subscribe(DIRECT_MESSAGING, CLUSTER_PEER_SERIALIZER::decode, (Endpoint endpoint, Payload payload) -> {
                //logger.info("Received message from " + payload.nodeAddress);
                messageListener.receive(payload.nodeAddress, payload.bytes);
            }, executor);
            atomix.clusterService().addListener(event -> refreshView(atomix));
            refreshView(atomix);
        }));
    }

    private void refreshView(Atomix atomix) {
        final Set<Node> clusterNodes = atomix.clusterService().getNodes();
        final Map<NodeAddress, NodeId> tmp = new ConcurrentHashMap<>(clusterNodes.size());
        for (final Node node : clusterNodes) {
            tmp.put(new NodeAddressImpl(nameUUIDFromBytes(node.id().id().getBytes())), node.id());
        }
        nodeMap.values().retainAll(tmp.values());
        nodeMap.putAll(tmp);
        logger.info("New view " + nodeMap.keySet());

        viewListener.onViewChange(nodeMap.keySet());
    }

    private static Atomix createAtomicNode(String nodeId, int port, Node[] bootstrapNodes) {
        Atomix.Builder builder = Atomix.builder();
        builder.withLocalNode(Node.builder(nodeId)
                .withType(Node.Type.DATA)
                .withEndpoint(new Endpoint(getLocalAddress(), port))
                .build());

        logger.info("Creating " + nodeId + " node on port " + port + " with bootstrap nodes " + Arrays.toString(bootstrapNodes));

        return builder.withDataDirectory(new File(System.getProperty("user.dir"), "data/" + nodeId)).withBootstrapNodes(bootstrapNodes).build();
    }

    private static InetAddress getLocalAddress() {
        try {
            return InetAddress.getLocalHost();  // first NIC
        } catch (Exception ignore) {

        }
        try {
            return InetAddress.getByName(null);
        } catch (UnknownHostException ignore) {
            throw new RuntimeException("Failed to find address for binding");
        }
    }

    @Override
    public void leave() {
        logger.info("Invoking stop..");
        atomix.stop().join();
    }
}
