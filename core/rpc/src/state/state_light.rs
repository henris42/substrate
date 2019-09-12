// Copyright 2019 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

//! State API backend for light nodes.

use std::{
	sync::Arc,
	collections::HashMap,
};
use codec::Decode;
use futures03::{
	future::{ready, Either},
	channel::oneshot::{channel, Sender},
	FutureExt, TryFutureExt,
	StreamExt as _, TryStreamExt as _,
};
use hash_db::Hasher;
use jsonrpc_pubsub::{typed::Subscriber, SubscriptionId};
use log::warn;
use parking_lot::Mutex;
use rpc::{
	Result as RpcResult,
	futures::Sink,
	futures::future::{result, Future},
	futures::stream::{futures_ordered, Stream},
};

use api::Subscriptions;
use client::{
	BlockchainEvents, Client, CallExecutor, backend::Backend,
	error::Error as ClientError,
	light::{
		blockchain::{future_header, RemoteBlockchain},
		fetcher::{Fetcher, RemoteCallRequest, RemoteReadRequest, RemoteReadChildRequest},
	},
};
use primitives::{
	H256, Blake2Hasher, Bytes, OpaqueMetadata,
	storage::{StorageKey, StorageData, StorageChangeSet},
};
use runtime_version::RuntimeVersion;
use sr_primitives::{
	generic::BlockId,
	traits::Block as BlockT,
};

use super::{StateBackend, error::{FutureResult, Error}, client_err};

/// State API backend for light nodes.
pub struct LightState<Block: BlockT, F: Fetcher<Block>, B, E, RA> {
	client: Arc<Client<B, E, Block, RA>>,
	subscriptions: Subscriptions,
	shared_runtime_version_requests: SharedRequests<Block::Hash, (), RuntimeVersion>,
	shared_storage_requests: SharedRequests<Block::Hash, StorageKey, Option<StorageData>>,
	remote_blockchain: Arc<dyn RemoteBlockchain<Block>>,
	fetcher: Arc<F>,
}

/// Map of shared requests. Requests are grouped by block hash, then by key.
/// Requests are shared if both block and key match.
type SharedRequests<Hash, K, V> = Arc<Mutex<
	HashMap<
		Hash,
		HashMap<
			K,
			Vec<Sender<Result<V, ()>>>
		>,
	>,
>>;

impl<Block: BlockT, F: Fetcher<Block> + 'static, B, E, RA> LightState<Block, F, B, E, RA>
	where
		Block: BlockT<Hash=H256>,
		B: Backend<Block, Blake2Hasher> + Send + Sync + 'static,
		E: CallExecutor<Block, Blake2Hasher> + Send + Sync + 'static + Clone,
		RA: Send + Sync + 'static,
{
	/// Create new state API backend for light nodes.
	pub fn new(
		client: Arc<Client<B, E, Block, RA>>,
		subscriptions: Subscriptions,
		remote_blockchain: Arc<dyn RemoteBlockchain<Block>>,
		fetcher: Arc<F>,
	) -> Self {
		Self {
			client,
			subscriptions,
			shared_runtime_version_requests: Arc::new(Mutex::new(HashMap::new())),
			shared_storage_requests: Arc::new(Mutex::new(HashMap::new())),
			remote_blockchain,
			fetcher,
		}
	}

	/// Returns given block hash or best block hash if None is passed.
	fn block_or_best(&self, hash: Option<Block::Hash>) -> Block::Hash {
		hash.unwrap_or_else(|| self.client.info().chain.best_hash)
	}
}

impl<Block, F, B, E, RA> StateBackend<B, E, Block, RA> for LightState<Block, F, B, E, RA>
	where
		Block: BlockT<Hash=H256>,
		B: Backend<Block, Blake2Hasher> + Send + Sync + 'static,
		E: CallExecutor<Block, Blake2Hasher> + Send + Sync + 'static + Clone,
		RA: Send + Sync + 'static,
		F: Fetcher<Block> + 'static
{
	fn call(
		&self,
		block: Option<Block::Hash>,
		method: String,
		call_data: Bytes,
	) -> FutureResult<Bytes> {
		Box::new(call(
			&*self.remote_blockchain,
			self.fetcher.clone(),
			self.block_or_best(block),
			method,
			call_data,
		).boxed().compat())
	}

	fn storage_keys(
		&self,
		_block: Option<Block::Hash>,
		_prefix: StorageKey,
	) -> FutureResult<Vec<StorageKey>> {
		Box::new(result(Err(client_err(ClientError::NotAvailableOnLightClient))))
	}

	fn storage(
		&self,
		block: Option<Block::Hash>,
		key: StorageKey,
	) -> FutureResult<Option<StorageData>> {
		Box::new(storage(
			&*self.remote_blockchain,
			self.fetcher.clone(),
			self.block_or_best(block),
			key,
		).boxed().compat())
	}

	fn storage_hash(
		&self,
		block: Option<Block::Hash>,
		key: StorageKey,
	) -> FutureResult<Option<Block::Hash>> {
		Box::new(self
			.storage(block, key)
			.and_then(|maybe_storage|
				result(Ok(maybe_storage.map(|storage| Blake2Hasher::hash(&storage.0))))
			)
		)
	}

	fn child_storage_keys(
		&self,
		_block: Option<Block::Hash>,
		_child_storage_key: StorageKey,
		_prefix: StorageKey,
	) -> FutureResult<Vec<StorageKey>> {
		Box::new(result(Err(client_err(ClientError::NotAvailableOnLightClient))))
	}

	fn child_storage(
		&self,
		block: Option<Block::Hash>,
		child_storage_key: StorageKey,
		key: StorageKey,
	) -> FutureResult<Option<StorageData>> {
		let block = self.block_or_best(block);
		let fetcher = self.fetcher.clone();
		let child_storage = resolve_header(&*self.remote_blockchain, &*self.fetcher, block)
			.then(move |result| match result {
				Ok(header) => Either::Left(fetcher.remote_read_child(RemoteReadChildRequest {
					block,
					header,
					storage_key: child_storage_key.0,
					key: key.0,
					retry_count: Default::default(),
				}).then(|result| ready(result.map(|data| data.map(StorageData)).map_err(client_err)))),
				Err(error) => Either::Right(ready(Err(error))),
			});

		Box::new(child_storage.boxed().compat())
	}

	fn child_storage_hash(
		&self,
		block: Option<Block::Hash>,
		child_storage_key: StorageKey,
		key: StorageKey,
	) -> FutureResult<Option<Block::Hash>> {
		Box::new(self
			.child_storage(block, child_storage_key, key)
			.and_then(|maybe_storage|
				result(Ok(maybe_storage.map(|storage| Blake2Hasher::hash(&storage.0))))
			)
		)
	}

	fn metadata(&self, block: Option<Block::Hash>) -> FutureResult<Bytes> {
		let metadata = self.call(block, "Metadata_metadata".into(), Bytes(Vec::new()))
			.and_then(|metadata| OpaqueMetadata::decode(&mut &metadata.0[..])
				.map(Into::into)
				.map_err(|decode_err| client_err(ClientError::CallResultDecode(
					"Unable to decode metadata",
					decode_err,
				))));

		Box::new(metadata)
	}

	fn runtime_version(&self, block: Option<Block::Hash>) -> FutureResult<RuntimeVersion> {
		Box::new(runtime_version(
			&*self.remote_blockchain,
			self.fetcher.clone(),
			self.block_or_best(block),
		).boxed().compat())
	}

	fn query_storage(
		&self,
		_from: Block::Hash,
		_to: Option<Block::Hash>,
		_keys: Vec<StorageKey>,
	) -> FutureResult<Vec<StorageChangeSet<Block::Hash>>> {
		Box::new(result(Err(client_err(ClientError::NotAvailableOnLightClient))))
	}

	fn subscribe_storage(
		&self,
		_meta: crate::metadata::Metadata,
		subscriber: Subscriber<StorageChangeSet<Block::Hash>>,
		keys: Option<Vec<StorageKey>>
	) {
		let keys = match keys {
			Some(keys) => keys,
			None => {
				warn!("Cannot subscribe to all keys on light client. Subscription rejected.");
				return;
			}
		};

		self.subscriptions.add(subscriber, move |sink| {
			let fetcher = self.fetcher.clone();
			let remote_blockchain = self.remote_blockchain.clone();

			let changes_stream = subscription_stream::<Block, _, _, _, _, _, _, _>(
				self.block_or_best(None),
				keys,
				self.client.import_notification_stream()
					.map(|notification| Ok::<_, ()>(notification.hash))
					.compat(),
				self.shared_storage_requests.clone(),
				move |block, key| storage(
					&*remote_blockchain,
					fetcher.clone(),
					block,
					key.clone(),
				),
				|block, old_values, new_values| old_values
					.as_ref()
					.map(|old_values| if *old_values != new_values {
						Some(StorageChangeSet {
							block,
							changes: new_values
								.iter()
								.map(|(k, v)| (k.clone(), v.clone()))
								.collect(),
						})
					} else {
						None
					})
					.unwrap_or_else(|| Some(StorageChangeSet {
						block: block.clone(),
						changes: new_values
							.iter()
							.map(|(k, v)| (k.clone(), v.clone()))
							.collect(),
					})),
			);

			sink
				.sink_map_err(|e| warn!("Error sending notifications: {:?}", e))
				.send_all(changes_stream.map(|changes| Ok(changes)))
				// we ignore the resulting Stream (if the first stream is over we are unsubscribed)
				.map(|_| ())
		});
	}

	fn unsubscribe_storage(
		&self,
		_meta: Option<crate::metadata::Metadata>,
		id: SubscriptionId,
	) -> RpcResult<bool> {
		Ok(self.subscriptions.cancel(id))
	}

	fn subscribe_runtime_version(
		&self,
		_meta: crate::metadata::Metadata,
		subscriber: Subscriber<RuntimeVersion>,
	) {
		self.subscriptions.add(subscriber, move |sink| {
			let fetcher = self.fetcher.clone();
			let remote_blockchain = self.remote_blockchain.clone();

			let versions_stream = subscription_stream::<Block, _, _, _, _, _, _, _>(
				self.block_or_best(None),
				vec![()],
				self.client.import_notification_stream()
					.map(|notification| Ok::<_, ()>(notification.hash))
					.compat(),
				self.shared_runtime_version_requests.clone(),
				move |block, _| runtime_version(
					&*remote_blockchain,
					fetcher.clone(),
					block,
				),
				|_, old_version, new_version| old_version
					.as_ref()
					.map(|old_version| if *old_version != new_version {
						Some(new_version.clone())
					} else {
						None
					})
					.unwrap_or_else(|| Some(new_version.clone())),
			);

			sink
				.sink_map_err(|e| warn!("Error sending notifications: {:?}", e))
				.send_all(versions_stream
					.filter_map(|versions_map| versions_map
						.into_iter()
						.map(|(_, version)| version)
						.next()
					)
					.map(|version| Ok(version))
				)
				// we ignore the resulting Stream (if the first stream is over we are unsubscribed)
				.map(|_| ())
		});
	}

	fn unsubscribe_runtime_version(
		&self,
		_meta: Option<crate::metadata::Metadata>,
		id: SubscriptionId,
	) -> RpcResult<bool> {
		Ok(self.subscriptions.cancel(id))
	}
}

/// Resolve header by hash.
fn resolve_header<Block: BlockT, F: Fetcher<Block>>(
	remote_blockchain: &dyn RemoteBlockchain<Block>,
	fetcher: &F,
	block: Block::Hash,
) -> impl std::future::Future<Output = Result<Block::Header, Error>> {
	let maybe_header = future_header(
		remote_blockchain,
		fetcher,
		BlockId::Hash(block),
	);

	maybe_header.then(move |result|
		ready(result.and_then(|maybe_header|
			maybe_header.ok_or(ClientError::UnknownBlock(format!("{}", block)))
		).map_err(client_err)),
	)
}

/// Call runtime method at given block
fn call<Block: BlockT, F: Fetcher<Block>>(
	remote_blockchain: &dyn RemoteBlockchain<Block>,
	fetcher: Arc<F>,
	block: Block::Hash,
	method: String,
	call_data: Bytes,
) -> impl std::future::Future<Output = Result<Bytes, Error>> {
	resolve_header(remote_blockchain, &*fetcher, block)
		.then(move |result| match result {
			Ok(header) => Either::Left(fetcher.remote_call(RemoteCallRequest {
				block,
				header,
				method,
				call_data: call_data.0,
				retry_count: Default::default(),
			}).then(|result| ready(result.map(Bytes).map_err(client_err)))),
			Err(error) => Either::Right(ready(Err(error))),
		})
}

/// Get runtime version at given block.
fn runtime_version<Block: BlockT, F: Fetcher<Block>>(
	remote_blockchain: &dyn RemoteBlockchain<Block>,
	fetcher: Arc<F>,
	block: Block::Hash,
) -> impl std::future::Future<Output = Result<RuntimeVersion, Error>> {
	call(
		remote_blockchain,
		fetcher,
		block,
		"Core_version".into(),
		Bytes(Vec::new()),
	)
	.then(|version| ready(version.and_then(|version|
		Decode::decode(&mut &version.0[..]).map_err(|_| client_err(ClientError::VersionInvalid))
	)))
}

/// Get storage value at given key at given block.
fn storage<Block: BlockT, F: Fetcher<Block>>(
	remote_blockchain: &dyn RemoteBlockchain<Block>,
	fetcher: Arc<F>,
	block: Block::Hash,
	key: StorageKey,
) -> impl std::future::Future<Output = Result<Option<StorageData>, Error>> {
	resolve_header(remote_blockchain, &*fetcher, block)
		.then(move |result| match result {
			Ok(header) => Either::Left(fetcher.remote_read(RemoteReadRequest {
				block,
				header,
				key: key.0,
				retry_count: Default::default(),
			}).then(|result| ready(result.map(|data| data.map(StorageData)).map_err(client_err)))),
			Err(error) => Either::Right(ready(Err(error))),
		})
}

/// Returns subscription stream that issues request on every imported block and
/// if value has changed from previous block, emits (stream) item.
fn subscription_stream<Block, FutureBlocksStream, K, V, IssueRequest, IssueRequestFuture, N, CompareValues>(
	block: Block::Hash,
	keys: Vec<K>,
	future_blocks_stream: FutureBlocksStream,
	shared_requests: SharedRequests<Block::Hash, K, V>,
	issue_request: IssueRequest,
	compare_values: CompareValues,
) -> impl Stream<Item=N, Error=()> where
	Block: BlockT<Hash=H256>,
	FutureBlocksStream: Stream<Item=Block::Hash, Error=()>,
	K: Send + 'static + std::hash::Hash + Eq + Clone,
	V: Send + 'static + Clone,
	IssueRequest: 'static + Fn(Block::Hash, &K) -> IssueRequestFuture,
	IssueRequestFuture: std::future::Future<Output = Result<V, Error>> + Send + 'static,
	CompareValues: Fn(Block::Hash, Option<&HashMap<K, V>>, &HashMap<K, V>) -> Option<N>,
{
	// we need to send initial value first, then we'll only be sending if value has changed
	let previous_value = Arc::new(Mutex::new(None));

	// prepare a stream of blocks hashes we want values for
	let initial_block_future = result(Ok(block));
	let initial_block_stream = initial_block_future.into_stream();
	let blocks_stream = initial_block_stream.chain(future_blocks_stream);

	// now let's return changed values for selected blocks
	blocks_stream
		.and_then(move |block|
			// issue/reuse request for every key
			// (request may never fail, because we do not want to stop notifications stream
			// if request for single block+key has failed)
			futures_ordered(keys.iter().map(|key| {
				let key = key.clone();
				maybe_share_remote_request::<Block, _, _, _, _>(
					shared_requests.clone(),
					block,
					&key,
					&issue_request,
				)
				.then(move |response| ready(Ok(match response {
					Ok(response) => Some((key.clone(), response)),
					Err(err) => {
						warn!("Remote request for subscription data has failed with: {:?}", err);
						None
					},
				})))
				.boxed()
				.compat()
			}))
			.fold(
				(false, HashMap::new()),
				|(some_request_failed, mut new_values), key_and_value|
					match (some_request_failed, key_and_value) {
						(true, _) => result(Ok::<_, ()>((true, new_values))),
						(false, Some((key, value))) => {
							new_values.insert(key, value);
							result(Ok::<_, ()>((false, new_values)))
						},
						(false, None) => result(Ok::<_, ()>((true, HashMap::new())))
					},
			)
			.map(|(some_request_failed, new_values)| if !some_request_failed {
				Some(new_values)
			} else {
				None
			})
		)
		.filter_map(move |new_values| new_values.and_then(|new_values| {
			let mut previous_value = previous_value.lock();
			compare_values(block, previous_value.as_ref(), &new_values)
				.map(|notification_value| {
					*previous_value = Some(new_values);
					notification_value
				})
		}))
		.map_err(|_| ())
}

/// Request some data from remote node, probably reusing response from already
/// (in-progress) existing request.
fn maybe_share_remote_request<Block: BlockT, K, V, IssueRequest, IssueRequestFuture>(
	shared_requests: SharedRequests<Block::Hash, K, V>,
	block: Block::Hash,
	key: &K,
	issue_request: &IssueRequest,
) -> impl std::future::Future<Output = Result<V, ()>> where
	K: std::hash::Hash + Eq + Clone,
	V: Clone,
	IssueRequest: Fn(Block::Hash, &K) -> IssueRequestFuture,
	IssueRequestFuture: std::future::Future<Output = Result<V, Error>>,
{
	let (sender, receiver) = channel();
	let active_requests = shared_requests.clone();
	let mut active_requests = active_requests.lock();
	let active_block_requests = active_requests.entry(block).or_insert_with(|| HashMap::new());
	let active_key_requests = active_block_requests.entry(key.clone()).or_default();
	active_key_requests.push(sender);

	// if that isn't the first request - just listen for existing request' response
	if active_key_requests.len() != 1 {
		return Either::Right(receiver.then(|r| ready(r.unwrap_or(Err(())))));
	}

	// that is the first request - issue remote request + notify all listeners on
	// completion
	let key = key.clone();
	Either::Left(
		issue_request(block, &key)
			.then(move |remote_result| {
				let remote_result = remote_result.map_err(|_| ());
				let mut shared_requests = shared_requests.lock();
				if let Some(mut shared_requests_at) = shared_requests.remove(&block) {
					if let Some(shared_requests_for) = shared_requests_at.remove(&key) {
						// skip first element, because this future is the first element
						for receiver in shared_requests_for.into_iter().skip(1) {
							if let Err(_) = receiver.send(remote_result.clone()) {
								// we don't care if receiver has been dropped already
							}
						}
					}
				}

				ready(remote_result)
			})
	)
}

#[cfg(test)]
mod tests {
	use test_client::runtime::Block;
	use super::*;

	#[test]
	fn subscription_stream_works() {
		let shared_requests = Arc::new(Mutex::new(HashMap::new()));
		let stream = subscription_stream::<Block, _, _, _, _, _, _, _>(
			H256::from([1; 32]),
			vec![()],
			futures_ordered(vec![result(Ok(H256::from([2; 32]))), result(Ok(H256::from([3; 32])))]),
			shared_requests,
			|block, _| match block[0] {
				1 | 2 => ready(Ok(100)),
				3 => ready(Ok(200)),
				_ => unreachable!("should not issue additional requests"),
			},
			|_, old_value, new_value| match old_value == Some(new_value) {
				true => None,
				false => Some(new_value.clone()),
			}
		);

		assert_eq!(
			stream.filter_map(|map| map.into_iter().map(|(_, v)| v).next()).collect().wait(),
			Ok(vec![100, 200])
		);
	}

	#[test]
	fn subscription_stream_ignores_failed_requests() {
		let shared_requests = Arc::new(Mutex::new(HashMap::new()));
		let stream = subscription_stream::<Block, _, _, _, _, _, _, _>(
			H256::from([1; 32]),
			vec![()],
			futures_ordered(vec![result(Ok(H256::from([2; 32]))), result(Ok(H256::from([3; 32])))]),
			shared_requests,
			|block, _| match block[0] {
				1 => ready(Ok(100)),
				2 => ready(Err(client_err(ClientError::NotAvailableOnLightClient))),
				3 => ready(Ok(200)),
				_ => unreachable!("should not issue additional requests"),
			},
			|_, old_value, new_value| match old_value == Some(new_value) {
				true => None,
				false => Some(new_value.clone()),
			}
		);

		assert_eq!(
			stream.filter_map(|map| map.into_iter().map(|(_, v)| v).next()).collect().wait(),
			Ok(vec![100, 200])
		);
	}

	#[test]
	fn maybe_share_remote_request_shares_request() {
		type UnreachableFuture = futures03::future::Ready<Result<u32, Error>>;

		let shared_requests = Arc::new(Mutex::new(HashMap::new()));

		// let's 'issue' requests for (B1, K1) and (B2, K2)
		shared_requests.lock().insert(
			H256::from([1; 32]),
			vec![(1, vec![channel().0])].into_iter().collect(),
		);
		shared_requests.lock().insert(
			H256::from([2; 32]),
			vec![(2, vec![channel().0])].into_iter().collect(),
		);

		// make sure that no additional requests are issued when we're asking for (B1, K1)
		let _ = maybe_share_remote_request::<Block, _, _, _, UnreachableFuture>(
			shared_requests.clone(),
			H256::from([1; 32]),
			&1,
			&|_, _| unreachable!("no duplicate requests issued"),
		);

		// make sure that no additional requests are issued when we're asking for (B2, K2)
		let _ = maybe_share_remote_request::<Block, _, _, _, UnreachableFuture>(
			shared_requests.clone(),
			H256::from([2; 32]),
			&2,
			&|_, _| unreachable!("no duplicate requests issued"),
		);

		// make sure that additional requests is issued when we're asking for (B1, K2)
		let request_issued = Arc::new(Mutex::new(false));
		let _ = maybe_share_remote_request::<Block, _, _, _, UnreachableFuture>(
			shared_requests.clone(),
			H256::from([1; 32]),
			&2,
			&|_, _| {
				*request_issued.lock() = true;
				ready(Ok(42))
			},
		);
		assert!(*request_issued.lock());

		// make sure that additional requests is issued when we're asking for (B2, K1)
		let request_issued = Arc::new(Mutex::new(false));
		let _ = maybe_share_remote_request::<Block, _, _, _, UnreachableFuture>(
			shared_requests.clone(),
			H256::from([2; 32]),
			&1,
			&|_, _| {
				*request_issued.lock() = true;
				ready(Ok(42))
			},
		);
		assert!(*request_issued.lock());
	}
}
