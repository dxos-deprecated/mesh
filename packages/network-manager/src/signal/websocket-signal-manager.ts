//
// Copyright 2020 DXOS.org
//

import debug from 'debug';

import { Event, synchronized } from '@dxos/async';
import { PublicKey } from '@dxos/crypto';
import { ComplexMap, ComplexSet } from '@dxos/util';

import { SignalManager } from './interface';
import { SignalApi } from './signal-api';

const log = debug('dxos:network-manager:websocket-signal-manager');

export class WebsocketSignalManager implements SignalManager {
  private readonly _servers = new Map<string, SignalApi>();

  /** Topics joined: topic => peerId */
  private readonly _topicsJoined = new ComplexMap<PublicKey, PublicKey>(x => x.toHex());

  private readonly _topicsJoinedPerSignal = new Map<string, ComplexMap<PublicKey, PublicKey>>();

  /** host => topic => peers */
  private readonly _peerCandidates = new Map<string, ComplexMap<PublicKey, ComplexSet<PublicKey>>>();

  private _reconcileTimeoutId?: NodeJS.Timeout;

  readonly statusChanged = new Event<SignalApi.Status[]>();

  readonly commandTrace = new Event<SignalApi.CommandTrace>();

  constructor (
    private readonly _hosts: string[],
    private readonly _onOffer: (message: SignalApi.SignalMessage) => Promise<SignalApi.Answer>
  ) {
    log(`Created WebsocketSignalManager with signal servers: ${_hosts}`);
    for (const host of this._hosts) {
      const server = new SignalApi(
        host,
        async (msg) => this._onOffer(msg),
        async msg => {
          this.onSignal.emit(msg);
        }
      );
      this._servers.set(host, server);
      server.statusChanged.on(() => this.statusChanged.emit(this.getStatus()));
      server.commandTrace.on(trace => this.commandTrace.emit(trace));
      this._topicsJoinedPerSignal.set(host, new ComplexMap(x => x.toHex()));
      this._peerCandidates.set(host, new ComplexMap(x => x.toHex()));
    }
  }

  getStatus (): SignalApi.Status[] {
    return Array.from(this._servers.values()).map(server => server.getStatus());
  }

  join (topic: PublicKey, peerId: PublicKey) {
    log(`Join ${topic} ${peerId}`);
    this._topicsJoined.set(topic, peerId);
    this._reconcileJoinedTopics();
  }

  leave (topic: PublicKey, peerId: PublicKey) {
    log(`Leave ${topic} ${peerId}`);
    this._topicsJoined.delete(topic);
    this._reconcileJoinedTopics();
  }

  @synchronized
  private async _reconcileJoinedTopics () {
    log('Reconciling joined topics');
    const promises: Promise<void>[] = [];
    for (const [host, server] of this._servers.entries()) {
      for (const [topic, peerId] of this._topicsJoined.entries()) {
        if (!this._topicsJoinedPerSignal.get(host)!.has(topic)) {
          log(`Join ${topic} as ${peerId} on ${host}`);
          if(!this._peerCandidates.get(host)!.has(topic)) {
            this._peerCandidates.get(host)!.set(topic, new ComplexSet(x => x.toHex()));
          }
          promises.push(server.join(topic, peerId).then(
            peers => {
              log(`Joined successfully ${host}`);
              this._topicsJoinedPerSignal.get(host)!.set(topic, peerId);

              this._updatePeerCandidates(host, topic, peers)
            },
            err => {
              log(`Join error ${host} ${err.message}`);
              this._topicsJoinedPerSignal.get(host)!.delete(topic);
              this._reconcile();
            }
          ));
        }

        for (const [topic, peerId] of this._topicsJoinedPerSignal.get(host)!.entries()) {
          if (!this._topicsJoined.has(topic)) {
            log(`Leave ${topic} as ${peerId} on ${host}`);
            this._peerCandidates.get(host)!.delete(topic);
            promises.push(server.leave(topic, peerId).then(
              () => {
                log(`Left successfully ${host}`);
                this._topicsJoinedPerSignal.get(host)!.delete(topic);
              },
              err => {
                log(`Leave error ${host} ${err.message}`);
                this._reconcile();
              }
            ));
          }
        }
      }
    }
    await Promise.all(promises);
  }

  private _reconcile () {
    if (this._reconcileTimeoutId !== undefined) {
      return;
    }
    log('Will reconcile in 3 seconds');
    this._reconcileTimeoutId = setTimeout(() => {
      this._reconcileTimeoutId = undefined;
      this._reconcileJoinedTopics();
    }, 3_000);
  }

  lookup (topic: PublicKey) {
    log(`Lookup ${topic}`);
    for (const [host, server] of this._servers.entries()) {
      server.lookup(topic).then(
        peers => {
          this._updatePeerCandidates(host, topic, peers)
        },
        () => {
          // Error will already be reported in devtools. No need to do anything here.
        }
      );
    }
  }

  private _updatePeerCandidates(host: string, topic: PublicKey, peers: PublicKey[]) {
    log(`Peer candidates changed ${host} ${topic} ${peers}`);
    const candidatesSet = this._peerCandidates.get(host)!.get(topic);
    if(candidatesSet) {
      candidatesSet.clear();
      peers.forEach(peer => candidatesSet.add(peer));
    }

    const allPeers = new ComplexSet<PublicKey>(x => x.toHex());
    for(const peerMap of this._peerCandidates.values()) {
      const peers = peerMap.get(topic);
      if(peers) {
        for(const peer of peers) {
          allPeers.add(peer);
        }
      }
    }

    this.peerCandidatesChanged.emit([topic, Array.from(allPeers)]);
  }

  offer (msg: SignalApi.SignalMessage): Promise<SignalApi.Answer> {
    log(`Offer ${msg.remoteId}`);
    // Send offer to all signal servers, first successful response is returned to the caller.
    return new Promise((resolve, reject) => {
      const serverCount = this._servers.size;
      let errorCount = 0;
      for(const server of this._servers.values()) {
        server.offer(msg).then(
          answer => {
            log(`Offer answer ${JSON.stringify(answer)}`);
            // Only first call to resolve is processed.
            resolve(answer);
          },
          error => {
            log(`Offer error ${error.message}`);
            if(++errorCount === serverCount) {
              // Reject if all servers have rejected.
              reject(error);
            }
          }
        )
      }
    })
  }

  signal (msg: SignalApi.SignalMessage) {
    log(`Signal ${msg.remoteId}`);
    for (const server of this._servers.values()) {
      // Error should already be handled by devtools.
      server.signal(msg);
    }
  }

  peerCandidatesChanged = new Event<[topic: PublicKey, candidates: PublicKey[]]>()

  onSignal = new Event<SignalApi.SignalMessage>();
}
