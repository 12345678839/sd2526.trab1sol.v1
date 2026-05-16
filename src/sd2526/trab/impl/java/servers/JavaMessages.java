package sd2526.trab.impl.java.servers;

import static sd2526.trab.api.java.Result.error;
import static sd2526.trab.api.java.Result.ok;
import static sd2526.trab.api.java.Result.ErrorCode.BAD_REQUEST;
import static sd2526.trab.api.java.Result.ErrorCode.FORBIDDEN;
import static sd2526.trab.api.java.Result.ErrorCode.INTERNAL_ERROR;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import sd2526.trab.api.Message;
import sd2526.trab.api.User;
import sd2526.trab.api.java.Messages;
import sd2526.trab.api.java.Result;
import sd2526.trab.api.java.Result.ErrorCode;
import sd2526.trab.impl.api.java.AdminMessages;
import sd2526.trab.impl.db.DB;
import sd2526.trab.impl.java.clients.Clients;
import sd2526.trab.impl.utils.IP;

public class JavaMessages extends JavaBaseService implements Messages, AdminMessages {

	private static final int REMOTE_COMM_DEADLINE = 90000;
	private static final long MESSAGES_CACHE_EXPIRATION = 30000;
	private static final long DIRTY_INBOX_CACHE_EXPIRATION = 10000;
	private static final long REMOVED_INBOX_ENTRY_TTL = 300000;

	final JobDispatcher jobs;
	final AtomicLong counter = new AtomicLong(0L);
	private static Logger Log = Logger.getLogger(JavaMessages.class.getName());

	protected final Cache<String, Message> messagesCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(MESSAGES_CACHE_EXPIRATION))
			.build();

	protected final Cache<String, String> gcDeletedMessageCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(DIRTY_INBOX_CACHE_EXPIRATION))
			.removalListener((removed) -> {
				var sqlExpr = "SELECT * FROM Message m WHERE NOT EXISTS (SELECT 1 FROM InboxEntry e WHERE e.mid = m.id)";
				DB.transaction((hibernate) -> hibernate.select(sqlExpr, Message.class)
						.thenWith((orphans) -> hibernate.deleteMany(orphans)));
			})
			.build();

	protected final Cache<String, Boolean> removedInboxEntries = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(REMOVED_INBOX_ENTRY_TTL))
			.build();

	public final ConcurrentHashMap<String, Boolean> deliveredMessages = new ConcurrentHashMap<>();

	private JavaMessages() {
		this.jobs = new JobDispatcher();
		DB.select("SELECT m.mid FROM InboxEntry m WHERE m.recipient = '__warmup__'", String.class);
	}

	public Result<String> directPost(String pwd, Message msg) {
		if (msg == null || msg.getId() == null)
			return error(BAD_REQUEST);

		if (deliveredMessages.containsKey(msg.getId()))
			return ok(msg.getId());

		try {
			String[] parts = msg.getId().split("\\+");
			if (parts.length == 2) {
				long n = Long.parseLong(parts[1]);
				counter.updateAndGet(c -> Math.max(c, n));
			}
		} catch (Exception ignored) {
		}

		messagesCache.put(msg.getId(), msg);
		if (msg.originId() != null)
			messagesCache.put(msg.originId(), msg);

		var localAddresses = getLocalRecipientAddresses(msg);
		if (!localAddresses.isEmpty())
			deliverToKnownLocalRecipients(localAddresses, msg);

		deliveredMessages.put(msg.getId(), true);
		return ok(msg.getId());
	}

	public void syncCounterFromDB() {
		try {
			var result = DB.select("SELECT m.id FROM Message m", String.class);
			if (result.isOK() && !result.value().isEmpty()) {
				String localPrefix = THIS_DOMAIN + "+";
				long maxId = result.value().stream()
						.filter(id -> id != null && id.startsWith(localPrefix)).mapToLong(id -> {
							try {
								return Long.parseLong(id.split("\\+")[1]);
							} catch (Exception e) {
								return 0L;
							}
						})
						.max()
						.orElse(0L);

				long updated = counter.updateAndGet(c -> Math.max(c, maxId));
				Log.info("Counter synchronized from DB to: " + updated);
			}
		} catch (Exception e) {
			Log.warning("Failed to sync counter from DB: " + e.getMessage());
		}
	}

	@Override
	public Result<String> postMessage(String pwd, Message msg) {
		return getUser(msg.getSender(), pwd)
				.thenWith((user) -> doAsyncPost(user, msg));
	}

	@Override
	public Result<Message> getInboxMessage(String name, String mid, String pwd) {
		if (badParams(name, mid, pwd))
			return error(BAD_REQUEST);

		return getUser(name, pwd)
				.then(() -> DB.getOne(new InboxEntry(mid, name), InboxEntry.class))
				.then(() -> DB.getOne(mid, Message.class));
	}

	@Override
	public Result<List<String>> getAllInboxMessages(String name, String pwd) {
		var sqlExpr = "SELECT m.mid FROM InboxEntry m WHERE m.recipient = '%s'".formatted(name);
		return getUser(name, pwd)
				.then(() -> DB.select(sqlExpr, String.class));
	}

	@Override
	public Result<List<String>> searchInbox(String name, String pwd, String query) {
		String escapedQuery = query.toUpperCase().replace("'", "''");
		var sqlExpr = """
				SELECT e.mid FROM InboxEntry e
				INNER JOIN Message m ON e.mid = m.id
				WHERE e.recipient = '%s'
				AND (upper(m.subject) LIKE '%%%s%%' OR upper(m.contents) LIKE '%%%s%%')
				""".formatted(name, escapedQuery, escapedQuery);

		return getUser(name, pwd)
				.then(() -> DB.select(sqlExpr, String.class));
	}

	@Override
	public Result<Void> removeInboxMessage(String name, String mid, String pwd) {
		return getUser(name, pwd)
				.then(() -> {
					gcDeletedMessageCache.put(mid, mid);
					removedInboxEntries.put(mid + ":" + name, true);

					DB.deleteOne(new InboxEntry(mid, name));

					return Result.ok((Void) null);
				});
	}

	@Override
	public Result<Void> deleteMessage(String name, String mid, String pwd) {
		return getUser(name, pwd)
				.then(() -> getCachedMessage(mid))
				.thenWith(msg -> name.equals(getName(msg.senderAddress())) ? ok(msg) : error(FORBIDDEN))
				.thenWith((msg) -> doAsyncDelete(msg));
	}

	protected Result<User> getUser(String user, String pwd) {
		try {
			var name = user.split("@", 2)[0];
			return Clients.UsersClient.get().getUser(name, pwd);
		} catch (Exception x) {
			return Result.error(INTERNAL_ERROR);
		}
	}

	protected Result<Set<String>> checkUsers(Collection<String> addresses) {
		return Clients.AdminUsersClient.get().checkUsers(addresses);
	}

	private void deliverToKnownLocalRecipients(Collection<String> addresses, Message msg) {
		if (gcDeletedMessageCache.getIfPresent(msg.getId()) != null) {
			return;
		}

		DB.transaction((hibernate) -> {
			hibernate.persistOne(msg);
			for (var address : addresses) {
				var name = getName(address);
				var tombstoneKey = msg.getId() + ":" + name;

				if (removedInboxEntries.getIfPresent(tombstoneKey) == null) {
					hibernate.persistOne(new InboxEntry(msg.getId(), name));
				}
			}
			return ok();
		});
	}

	private void reportUnknownLocalRecipients(Collection<String> addresses, Message msg) {
		var senderAddress = msg.senderAddress();
		var senderDomain = super.getDomain(senderAddress);
		try {
			for (var recipientAddress : addresses) {
				var errorMsg = msg.cloneWithUserNotFound(recipientAddress);
				if (super.isLocalAddress(senderAddress)) {
					DB.transaction((hibernate) -> {
						hibernate.persistOne(errorMsg);
						hibernate.persistOne(new InboxEntry(errorMsg.getId(), getName(senderAddress)));
						return ok();
					});
				} else
					doAsyncRemotePost(senderDomain, errorMsg);
			}
		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	private Result<Void> postToLocalInboxes(Collection<String> addresses, Message msg) {
		return checkUsers(addresses)
				.thenWith(unknownAddresses -> {
					var knownAddresses = new HashSet<>(addresses);
					knownAddresses.removeAll(unknownAddresses);
					if (knownAddresses.size() > 0)
						deliverToKnownLocalRecipients(knownAddresses, msg);
					if (unknownAddresses.size() > 0)
						reportUnknownLocalRecipients(unknownAddresses, msg);
					return ok();
				});
	}

	@Override
	public Result<Void> remotePostMessage(Message msg) {
		return postToLocalInboxes(getLocalRecipientAddresses(msg), msg);
	}

	private Result<Void> deleteFromLocalInbox(String mid) {
		gcDeletedMessageCache.put(mid, mid);

		var sql = "SELECT * FROM InboxEntry e WHERE e.mid = '%s'".formatted(mid);
		return DB.transaction(hibernate -> {
			hibernate.select(sql, InboxEntry.class)
					.thenWith((entries) -> hibernate.deleteMany(entries));

			return hibernate.getOne(mid, Message.class)
					.thenWith(msg -> hibernate.deleteOne(msg))
					.mapToVoid();
		});
	}

	@Override
	public Result<Void> remoteDeleteMessage(String mid) {
		return deleteFromLocalInbox(mid);
	}

	protected Result<Message> getCachedMessage(String mid) {
		var msg = messagesCache.getIfPresent(mid);
		return msg != null ? ok(msg) : error(FORBIDDEN);
	}

	public final class JobDispatcher {
		private final ConcurrentHashMap<String, ExecutorService> executors = new ConcurrentHashMap<>();

		public void submit(String domain, Runnable job) {
			ExecutorService executor = executors.computeIfAbsent(domain, d -> Executors.newSingleThreadExecutor(r -> {
				Thread t = new Thread(r);
				t.setUncaughtExceptionHandler((thr, ex) -> ex.printStackTrace());
				return t;
			}));
			executor.submit(job);
		}
	}

	public Result<String> doAsyncPost(User sender, Message msg) {
		if (msg.getId() != null) {
			if (deliveredMessages.containsKey(msg.getId()))
				return ok(msg.getId());

			try {
				long n = Long.parseLong(msg.getId().split("\\+")[1]);
				counter.updateAndGet(c -> Math.max(c, n));
			} catch (Exception ignored) {
			}

			if (!msg.getSender().contains("<")) {
				msg.setSender("%s <%s@%s>".formatted(
						sender.getDisplayName(), sender.getName(), sender.getDomain()));
			}

			messagesCache.put(msg.getId(), msg);
			if (msg.originId() != null)
				messagesCache.put(msg.originId(), msg);
			var localAddresses = getLocalRecipientAddresses(msg);
			if (!localAddresses.isEmpty())
				postToLocalInboxes(localAddresses, msg);

			deliveredMessages.put(msg.getId(), true);
			return Result.ok(msg.getId());
		}

		return getCachedMessage(msg.originId()).mapValue(Message::getId).orElse(() -> {
			msg.setId("%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet()));
			messagesCache.put(msg.originId(), new Message(msg));
			msg.setSender("%s <%s@%s>".formatted(
					sender.getDisplayName(), sender.getName(), sender.getDomain()));
			messagesCache.put(msg.getId(), msg);
			var localAdresses = getLocalRecipientAddresses(msg);
			var remoteAddresses = getRemoteRecipientAddresses(msg);
			if (localAdresses.size() > 0)
				postToLocalInboxes(localAdresses, msg);
			if (remoteAddresses.size() > 0) {
				var remoteTargets = remoteAddresses.stream().collect(
						Collectors.groupingBy(super::getDomain,
								Collectors.mapping(address -> address, Collectors.toSet())));
				for (var e : remoteTargets.entrySet()) {
					jobs.submit(e.getKey(), () -> {
						var res = super.reTry(
								() -> Clients.AdminMessagesClient.get(e.getKey()).remotePostMessage(msg),
								REMOTE_COMM_DEADLINE);
						if (res.error() == ErrorCode.TIMEOUT) {
							for (var address : e.getValue())
								postToLocalInboxes(Set.of(msg.senderAddress()), msg.cloneWithTimeout(address));
						}
					});
				}
			}
			deliveredMessages.put(msg.getId(), true);
			return Result.ok(msg.getId());
		});
	}

	public Result<Void> doAsyncDelete(Message msg) {
		var domains = msg.getDestination().stream()
				.map(r -> r.split("@")[1])
				.collect(Collectors.toSet());
		for (var domain : domains)
			if (domain.equals(IP.domain()))
				deleteFromLocalInbox(msg.getId());
			else
				super.reTry(
						() -> Clients.AdminMessagesClient.get(domain).remoteDeleteMessage(msg.getId()),
						REMOTE_COMM_DEADLINE);
		return Result.ok();
	}

	public void doAsyncRemotePost(String remoteDomain, Message msg) {
		jobs.submit(remoteDomain, () -> {
			super.reTry(
					() -> Clients.AdminMessagesClient.get(remoteDomain).remotePostMessage(msg),
					REMOTE_COMM_DEADLINE);
		});
	}

	@Override
	public Result<Void> remoteDeleteUserInbox(String name) {
		var sqlExpr = "SELECT * FROM InboxEntry e WHERE e.recipient = '%s'".formatted(name);
		return DB.transaction(hibernate -> {
			return hibernate.select(sqlExpr, InboxEntry.class)
					.thenWith((entries) -> {
						hibernate.deleteMany(entries);
						for (var e : entries)
							gcDeletedMessageCache.put(e.mid, e.mid);
						return ok();
					});
		});
	}

	private List<String> getLocalRecipientAddresses(Message msg) {
		return msg.getDestination().stream().filter(super::isLocalAddress).toList();
	}

	private Set<String> getRemoteRecipientAddresses(Message msg) {
		return msg.getDestination().stream()
				.filter(Predicate.not(super::isLocalAddress))
				.collect(Collectors.toSet());
	}

	static JavaMessages instance;

	public static synchronized JavaMessages getInstance() {
		if (instance == null)
			instance = new JavaMessages();
		return instance;
	}

	public Result<Void> deleteLocalOnly(String mid) {
		return deleteFromLocalInbox(mid);
	}

	public Message getMessageFromCache(String mid) {
		return messagesCache.getIfPresent(mid);
	}

	public String generateNextId() {
		return "%s+%04d".formatted(THIS_DOMAIN, counter.incrementAndGet());
	}

	public boolean messageExists(String mid) {
		return deliveredMessages.containsKey(mid);
	}
}