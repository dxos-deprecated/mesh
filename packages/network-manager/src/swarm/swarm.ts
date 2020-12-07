import { discoveryKey, PublicKey } from "@dxos/crypto";
import { ComplexMap, ComplexSet } from "@dxos/util";
import { SignalApi } from '../signal/signal-api'
import assert from 'assert'
import { ProtocolProvider } from "../network-manager";
import { Connection } from "./connection";
import debug from 'debug';
import { SignalData } from "simple-peer";
import { Event } from "@dxos/async";
import { Topology } from "../topology/topology";

const log = debug('dxos:network-manager:swarm');

/**
 * A single peer's view of the swarm.
 * Manages a set of connections implemented by simple-peer instances.
 * Routes signal events and maintains swarm topology.
 */
export class Swarm {
  private readonly _connections = new ComplexMap<PublicKey, Connection>(x => x.toHex());

  private readonly _discoveredPeers = new ComplexSet<PublicKey>(x => x.toHex());

  get connections() {
    return Array.from(this._connections.values())
  }

  /**
   * New connection to a peer is started.
   */
  readonly connectionAdded = new Event<Connection>();

  /**
   * Connection to a peer is dropped.
   */
  readonly connectionRemoved = new Event<Connection>();

  /**
   * Connection is established to a new peer.
   */
  readonly connected = new Event<PublicKey>();

  constructor(
    private readonly _topic: PublicKey,
    private readonly _ownPeerId: PublicKey,
    private readonly _topology: Topology, // TODO(marik-d): Change topology at runtime.
    private readonly _protocol: ProtocolProvider,
    private readonly _sendOffer: (message: SignalApi.SignalMessage) => Promise<void>,
    private readonly _sendSignal: (message: SignalApi.SignalMessage) => Promise<void>,
    private readonly _lookup: () => void,
  ) {
    _topology.init({
      getState: () => ({
        connected: Array.from(this._connections.keys()),
        candidates: Array.from(this._discoveredPeers.keys()).filter(key => !this._connections.has(key)),
      }),
      connect: peer => this._initiateConnection(peer),
      disconnect: async peer => {
        try {
          await this._closeConnection(peer)
        } catch (err) {
          console.error('Error closing connection');
          console.error(err);
        }
        this._topology.update();
      },
      lookup: () => {
        this._lookup();
      }
    })
  }

  get ownPeerId() {
    return this._ownPeerId;
  }

  onCandidatesChanged(candidates: PublicKey[]) {
    this._discoveredPeers.clear();
    for(const candidate of candidates) {
      if(candidate.equals(this._ownPeerId)) continue;
      this._discoveredPeers.add(candidate);
    }
    this._topology.update();
  }

  async onOffer(message: SignalApi.SignalMessage): Promise<void> {
    // Id of the peer offering us the connection.
    const remoteId = message.id;
    assert(message.remoteId.equals(this._ownPeerId));
    assert(message.topic.equals(this._topic));

    // Check if we are already trying to connect to that peer.
    if(this._connections.has(message.id)) {
      // Peer with the highest Id closes it's connection, and accepts remote peer's offer.
      if(remoteId.toHex() < this._ownPeerId.toHex()) {
        this._closeConnection(message.id).catch(err => {
          console.error(err);
          // TODO(marik-d): Error handling.
        });
      } else {
        return;
      }
    }

    if(await this._topology.onOffer(message.id)) {
      this._createConnection(false, message.id, message.sessionId);
    }
    this._topology.update();
  }

  async onSignal(message: SignalApi.SignalMessage): Promise<void> {
    assert(message.remoteId.equals(this._ownPeerId));
    assert(message.topic.equals(this._topic));
    const connection = this._connections.get(message.id);
    if(!connection) {
      log(`Dropping signal message for non-existent connection: topic=${this._topic}, peerId=${message.id}`);
      return;
    }
    connection.signal(message);
  }

  private _initiateConnection(remoteId: PublicKey) {
    if(this._connections.has(remoteId)) {
      // Do nothing if peer is already connected.
      return;
    }

    const sessionId = PublicKey.random()

    this._createConnection(true, remoteId, sessionId);
    this._sendOffer({
      id: this._ownPeerId,
      remoteId,
      sessionId,
      topic: this._topic,
      data: {},
    })
    this._topology.update();
  }

  private _createConnection(initiator: boolean, remoteId: PublicKey, sessionId: PublicKey) {
    assert(!this._connections.has(remoteId), 'Peer already connected');
    let signals: SignalData[]
    const connection = new Connection(
      initiator,
      this._protocol({ channel: discoveryKey(this._topic) }),
      this._ownPeerId,
      remoteId,
      sessionId,
      this._topic,
      msg => this._sendSignal(msg),
    )
    this._connections.set(remoteId, connection)
    this.connectionAdded.emit(connection);
    Event.wrap(connection.peer, 'connect').once(() => this.connected.emit(remoteId));
    return connection;
  }

  private async _closeConnection(peerId: PublicKey) {
    const connection = this._connections.get(peerId);
    assert(connection);
    this._connections.delete(peerId);
    this.connectionRemoved.emit(connection);
    await connection.close();
  }
}