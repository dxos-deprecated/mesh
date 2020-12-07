import { PublicKey } from "@dxos/crypto";
import { Protocol } from "@dxos/protocol";
import { SignalApi } from "../signal/signal-api";
import SimplePeerConstructor, { Instance as SimplePeer, SignalData } from 'simple-peer';
import wrtc from 'wrtc';
import { Trigger } from "@dxos/util";
import assert from 'assert'
import debug from 'debug';
import { Event } from "@dxos/async";

const log = debug('dxos:network-manager:swarm:connection');

/**
 * Wrapper around simple-peer. Tracks peer state.
 */
export class Connection {
  private _state: Connection.State;
  private readonly _peer: SimplePeer;

  readonly stateChanged = new Event<Connection.State>();

  constructor(
    private readonly _initiator: boolean,
    private readonly _protocol: Protocol,
    private readonly _ownId: PublicKey,
    private readonly _remoteId: PublicKey,
    private readonly _sessionId: PublicKey,
    private readonly _topic: PublicKey,
    private readonly _sendSignal: (msg: SignalApi.SignalMessage) => Promise<void>
  ) {
    this._state = _initiator ? Connection.State.INITIATING_CONNECTION : Connection.State.WAITING_FOR_CONNECTION;
    this.stateChanged.emit(this._state);
    this._peer = new SimplePeerConstructor({
      initiator: _initiator,
      wrtc: SimplePeerConstructor.WEBRTC_SUPPORT ? undefined : wrtc,
    })
    this._peer.on('signal', async data => {
      try {
        await this._sendSignal({
          id: this._ownId,
          remoteId: this._remoteId,
          sessionId: this._sessionId,
          topic: this._topic,
          data,
        })
      } catch(err) {
        // TODO(marik-d): Error handling.
        console.error(err);
      }
    })
    this._peer.on('connect', () => {
      log(`Connection established ${this._ownId} -> ${this._remoteId}`)
      this._state = Connection.State.CONNECTED;
      this.stateChanged.emit(this._state);

      
      const stream = this._protocol.stream as any as NodeJS.ReadWriteStream;
      stream.pipe(this._peer).pipe(stream);
    })
    this._peer.on('error', err => {
      // TODO(marik-d): Error handling.
      console.error('peer error')
      console.error(err)
    })
    this._peer.on('close', () => {
      log(`Connection closed ${this._ownId} -> ${this._remoteId}`)
      this._state = Connection.State.CLOSED;
      this.stateChanged.emit(this._state);
      this._closeStream();
    })
    log(`Created connection ${this._ownId} -> ${this._remoteId} initiator=${this._initiator}`)
  }

  get remoteId() {
    return this._remoteId;
  }

  get state() {
    return this._state;
  }

  get peer() {
    return this._peer;
  }

  signal(msg: SignalApi.SignalMessage) {
    if(!msg.sessionId.equals(this._sessionId)) {
      log('Dropping signal for incorrect session id.');
      return;
    }
    if(msg.data.type === 'offer' && this._state === Connection.State.INITIATING_CONNECTION) {
      throw new Error('Invalid state: Cannot send offer to an initiating peer.');
    }
    assert(msg.id.equals(this._remoteId))
    assert(msg.remoteId.equals(this._ownId))
    log(`${this._ownId} received signal from ${this._remoteId}: ${msg.data.type}`)
    this._peer.signal(msg.data);
  }

  async close() {
    this._state = Connection.State.CLOSED;
    this.stateChanged.emit(this._state);
    this._closeStream();
    await new Promise(resolve => {
      this._peer.once('close', resolve);
      this._peer.destroy();
    });
  }

  private _closeStream() {
    const stream = this._protocol.stream as any as NodeJS.ReadWriteStream;
    stream.unpipe(this._peer).unpipe(stream);
  }
}

export namespace Connection {
  export enum State {
    INITIATING_CONNECTION = 'INITIATING_CONNECTION',
    WAITING_FOR_CONNECTION = 'WAITING_FOR_CONNECTION',
    CONNECTED = 'CONNECTED',
    CLOSED = 'CLOSED',
  }
}