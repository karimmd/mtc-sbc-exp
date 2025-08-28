# Flask services for peer communication

from flask import Flask, request, jsonify
import asyncio
import json
from typing import Dict, List, Any
import logging
import threading
from datetime import datetime


class PeerFlaskService:
    
    def __init__(self, peer_id: str, tier: str, port: int):
        self.peer_id = peer_id
        self.tier = tier
        self.port = port
        self.app = Flask(f"peer-{peer_id}")
        self.logger = logging.getLogger(f'FlaskService-{peer_id}')
        self.transaction_queue = []
        self.block_requests = []
        self.qos_requirements = {}
        
        self._setup_routes()
        
    def _setup_routes(self):
        # Setup Flask routes for peer communication
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            return jsonify({
                'status': 'healthy',
                'peer_id': self.peer_id,
                'tier': self.tier,
                'timestamp': datetime.now().isoformat()
            })
            
        @self.app.route('/transaction', methods=['POST'])
        def receive_transaction():
            transaction_data = request.json
            self.transaction_queue.append({
                'transaction': transaction_data,
                'received_at': datetime.now().isoformat(),
                'from_tier': request.headers.get('From-Tier', 'unknown')
            })
            
            return jsonify({
                'status': 'accepted',
                'peer_id': self.peer_id,
                'queue_length': len(self.transaction_queue)
            })
            
        @self.app.route('/block-verification', methods=['POST'])
        def block_verification_request():
            block_data = request.json
            self.block_requests.append({
                'block': block_data,
                'requested_at': datetime.now().isoformat(),
                'shard_id': block_data.get('shard_id', 'unknown')
            })
            
            # Simulate verification process
            verification_result = {
                'status': 'verified',
                'peer_id': self.peer_id,
                'block_hash': block_data.get('hash', 'missing'),
                'verified_at': datetime.now().isoformat()
            }
            
            return jsonify(verification_result)
            
        @self.app.route('/qos-requirements', methods=['POST', 'GET'])
        def handle_qos_requirements():
            if request.method == 'POST':
                qos_data = request.json
                slice_id = qos_data.get('slice_id')
                self.qos_requirements[slice_id] = {
                    'requirements': qos_data,
                    'received_at': datetime.now().isoformat(),
                    'tier': self.tier
                }
                
                return jsonify({
                    'status': 'qos_stored',
                    'slice_id': slice_id,
                    'peer_id': self.peer_id
                })
            else:
                return jsonify({
                    'peer_id': self.peer_id,
                    'tier': self.tier,
                    'qos_requirements': list(self.qos_requirements.keys())
                })
                
        @self.app.route('/shard-communication', methods=['POST'])
        def shard_communication():
            shard_data = request.json
            
            # Process cross-shard communication
            response = {
                'status': 'processed',
                'peer_id': self.peer_id,
                'source_shard': shard_data.get('source_shard'),
                'target_shard': shard_data.get('target_shard'),
                'processed_at': datetime.now().isoformat(),
                'tier_capacity': self._get_tier_capacity()
            }
            
            return jsonify(response)
            
        @self.app.route('/reputation-update', methods=['POST'])
        def reputation_update():
            reputation_data = request.json
            
            # Handle reputation updates from other peers
            result = {
                'status': 'reputation_updated',
                'peer_id': self.peer_id,
                'target_node': reputation_data.get('target_node'),
                'reputation_score': reputation_data.get('score'),
                'updated_at': datetime.now().isoformat()
            }
            
            return jsonify(result)
            
    def _get_tier_capacity(self):
        # Return tier-specific capacity information
        capacity_map = {
            'edge': {'cpu': '1-2 GHz', 'memory': '2-4 GB', 'storage': '32-128 GB'},
            'fog': {'cpu': '2-4 GHz', 'memory': '8-16 GB', 'storage': '256-512 GB'}, 
            'cloud': {'cpu': '4-8 GHz', 'memory': '32-64 GB', 'storage': '1-10 TB'}
        }
        return capacity_map.get(self.tier, {})
        
    def start_service(self):
        # Start Flask service in background thread
        def run_flask():
            self.app.run(
                host='0.0.0.0',
                port=self.port,
                debug=False,
                threaded=True
            )
            
        thread = threading.Thread(target=run_flask, daemon=True)
        thread.start()
        self.logger.info(f"Started Flask service for {self.peer_id} on port {self.port}")
        
    def get_status(self):
        # Get current service status
        return {
            'peer_id': self.peer_id,
            'tier': self.tier,
            'port': self.port,
            'transaction_queue_size': len(self.transaction_queue),
            'block_requests': len(self.block_requests),
            'qos_requirements': len(self.qos_requirements)
        }


class FlaskServiceManager:
    # Manages Flask services across all peers in the network
    
    def __init__(self):
        self.services = {}
        self.base_port = 5000
        self.logger = logging.getLogger('FlaskServiceManager')
        
    def create_peer_service(self, peer_id: str, tier: str) -> PeerFlaskService:
        # Create Flask service for a peer
        port = self.base_port + len(self.services)
        service = PeerFlaskService(peer_id, tier, port)
        self.services[peer_id] = service
        
        return service
        
    def start_all_services(self):
        # Start Flask services for all peers
        for peer_id, service in self.services.items():
            service.start_service()
            
        self.logger.info(f"Started {len(self.services)} Flask services")
        
    def get_service(self, peer_id: str) -> PeerFlaskService:
        # Get Flask service for a specific peer
        return self.services.get(peer_id)
        
    def get_services_by_tier(self, tier: str) -> List[PeerFlaskService]:
        # Get all services in a specific tier
        return [service for service in self.services.values() if service.tier == tier]
        
    def broadcast_to_tier(self, tier: str, endpoint: str, data: Dict):
        # Broadcast message to all peers in a tier
        services = self.get_services_by_tier(tier)
        results = []
        
        for service in services:
            # In real implementation, would make HTTP requests
            # Simulated here for blockchain purposes
            result = {
                'peer_id': service.peer_id,
                'status': 'broadcast_received',
                'endpoint': endpoint,
                'data_keys': list(data.keys())
            }
            results.append(result)
            
        return results