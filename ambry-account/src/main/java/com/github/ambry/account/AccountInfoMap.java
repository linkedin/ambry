package com.github.ambry.account;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 *   A class that represents a collection of {@link Account}s, where the ids and names of the
 *   {@link Account}s are one-to-one mapped. An {@code AccountInfoMap} guarantees no duplicated account
 *   id or name, nor conflict among the {@link Account}s within it.
 * </p>
 * <p>
 *   Based on the properties, a {@code AccountInfoMap} internally builds index for {@link Account}s using both
 *   {@link Account}'s id and name as key.
 * </p>
 */
class AccountInfoMap {
  private final Map<String, Account> nameToAccountMap;
  private final Map<Short, Account> idToAccountMap;
  // used to track last modified time of the accounts and containers in this cache
  private long lastModifiedTime = 0;
  private final static Logger logger = LoggerFactory.getLogger(AccountInfoMap.class);

  /**
   * Constructor for an empty {@code AccountInfoMap}.
   */
  AccountInfoMap(AccountServiceMetrics accountServiceMetrics) {
    this(accountServiceMetrics, new HashMap<>());
  }

  /**
   * <p>
   *   Constructs an {@code AccountInfoMap} from a group of {@link Account}s. The {@link Account}s exists
   *   in the form of a string-to-string map, where the key is the string form of an {@link Account}'s id,
   *   and the value is the string form of the {@link Account}'s JSON string.
   * </p>
   * <p>
   *   The source {@link Account}s in the {@code accountMap} may duplicate account ids or names, or corrupted
   *   JSON strings that cannot be parsed as valid {@link JSONObject}. In such cases, construction of
   *   {@code AccountInfoMap} will fail.
   * </p>
   * @param accountMap A map of {@link Account}s in the form of (accountIdString, accountJSONString).
   * @throws JSONException If parsing account data in json fails.
   */
  AccountInfoMap(AccountServiceMetrics accountServiceMetrics, Map<String, String> accountMap) throws JSONException {
    nameToAccountMap = new HashMap<>();
    idToAccountMap = new HashMap<>();
    for (Map.Entry<String, String> entry : accountMap.entrySet()) {
      String idKey = entry.getKey();
      String valueString = entry.getValue();
      Account account;
      JSONObject accountJson = new JSONObject(valueString);
      if (idKey == null) {
        accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        throw new IllegalStateException(
            "Invalid account record when reading accountMap in ZNRecord because idKey=null");
      }
      account = Account.fromJson(accountJson);
      if (account.getId() != Short.valueOf(idKey)) {
        accountServiceMetrics.remoteDataCorruptionErrorCount.inc();
        throw new IllegalStateException(
            "Invalid account record when reading accountMap in ZNRecord because idKey and accountId do not match. idKey="
                + idKey + " accountId=" + account.getId());
      }
      if (idToAccountMap.containsKey(account.getId()) || nameToAccountMap.containsKey(account.getName())) {
        throw new IllegalStateException(
            "Duplicate account id or name exists. id=" + account.getId() + " name=" + account.getName());
      }
      idToAccountMap.put(account.getId(), account);
      nameToAccountMap.put(account.getName(), account);
    }
  }

  /**
   * <p>
   *   Constructs an AccountInfoMap instance from one single JSON file which contains multiple accounts.
   * </p>
   * <p>
   *   This constructor is primarily useful if the source of the accounts is a flat storage for example a local
   *   JSON file.
   * </p>
   *
   * @param accountsJsonString JSON data containing an array of all accounts.
   * @throws JSONException If parsing account data in json fails.
   */
  protected AccountInfoMap(String accountsJsonString) throws JSONException {
    nameToAccountMap = new HashMap<>();
    idToAccountMap = new HashMap<>();

    JSONArray accountArray = new JSONArray(accountsJsonString);

    for (int i = 0; i < accountArray.length(); i++) {
      JSONObject accountJson = accountArray.getJSONObject(i);
      Account account = Account.fromJson(accountJson);

      if (idToAccountMap.containsKey(account.getId()) || nameToAccountMap.containsKey(account.getName())) {
        throw new IllegalStateException(
            "Duplicate account id or name exists. id=" + account.getId() + " name=" + account.getName());
      }

      idToAccountMap.put(account.getId(), account);
      nameToAccountMap.put(account.getName(), account);
    }
  }

  /**
   * <p>
   *   Constructs an {@code AccountInfoMap} from a group of {@link Account}s.
   * </p>
   * <p>
   *   This is used when source {@link Account}s is a relational database. We won't need to check for duplicate IDs or names
   *   since they are ensured by applying unique key constraints in the DB.
   * </p>
   * @param accounts A list of {@link Account}s.
   */
  AccountInfoMap(List<Account> accounts) {
    nameToAccountMap = new HashMap<>();
    idToAccountMap = new HashMap<>();
    updateAccounts(accounts);
  }

  /**
   * Gets {@link Account} by its id.
   * @param id The id to get the {@link Account}.
   * @return The {@link Account} with the given id, or {@code null} if such an {@link Account} does not exist.
   */
  Account getAccountById(Short id) {
    return idToAccountMap.get(id);
  }

  /**
   * Gets {@link Account} by its name.
   * @param name The id to get the {@link Account}.
   * @return The {@link Account} with the given name, or {@code null} if such an {@link Account} does not exist.
   */
  Account getAccountByName(String name) {
    return nameToAccountMap.get(name);
  }

  /**
   * Gets all the {@link Account}s in this {@code AccountInfoMap} in a {@link Collection}.
   * @return A {@link Collection} of all the {@link Account}s in this map.
   */
  Collection<Account> getAccounts() {
    return Collections.unmodifiableCollection(idToAccountMap.values());
  }

  /**
   * Return true if there is no accounts in this info map.
   * @return True when there is no accounts.
   */
  boolean isEmpty() {
    return idToAccountMap.isEmpty();
  }

  /**
   * Checks if there is any {@link Account} in a given collection of {@link Account}s conflicts against any {@link Account}
   * in a {@link AccountInfoMap}, according to the Javadoc of {@link AccountService}. Two {@link Account}s can be
   * conflicting with each other if they have different account Ids but the same account name.
   *
   * @param accountsToSet The collection of {@link Account}s to check conflict.
   * @return {@code true} if there is at least one {@link Account} in {@code accountPairs} conflicts with the existing
   *                      {@link Account}s in {@link AccountInfoMap}, {@code false} otherwise.
   */
  boolean hasConflictingAccount(Collection<Account> accountsToSet) {
    for (Account account : accountsToSet) {
      // if the account already exists, check that the snapshot version matches the expected value.
      Account accountInMap = getAccountById(account.getId());
      if (accountInMap != null && account.getSnapshotVersion() != accountInMap.getSnapshotVersion()) {
        logger.error(
            "Account to update (accountId={} accountName={}) has an unexpected snapshot version in zk (expected={}, encountered={})",
            account.getId(), account.getName(), account.getSnapshotVersion(), accountInMap.getSnapshotVersion());
        return true;
      }
      // check that there are no other accounts that conflict with the name of the account to update
      // (case D and E from the javadoc)
      Account potentialConflict = getAccountByName(account.getName());
      if (potentialConflict != null && potentialConflict.getId() != account.getId()) {
        logger.error(
            "Account to update (accountId={} accountName={}) conflicts with an existing record (accountId={} accountName={})",
            account.getId(), account.getName(), potentialConflict.getId(), potentialConflict.getName());
        return true;
      }
    }
    return false;
  }

  /**
   * Updates the {@code AccountInfoMap} with the input {@link Collection} of {@link Account}s.
   * @param accounts collection of {@link Account}s to be added.
   */
  void updateAccounts(Collection<Account> accounts) {
    for (Account updatedAccount : accounts) {
      Account account = idToAccountMap.get(updatedAccount.getId());
      if (account == null) {
        account = updatedAccount;
      } else {
        AccountBuilder accountBuilder = new AccountBuilder(account).status(updatedAccount.getStatus())
            .snapshotVersion(updatedAccount.getSnapshotVersion());
        updatedAccount.getAllContainers().forEach(accountBuilder::addOrUpdateContainer);
        account = accountBuilder.build();
      }
      idToAccountMap.put(account.getId(), account);
      nameToAccountMap.put(account.getName(), account);
    }
  }

  /**
   * Updates the {@code AccountInfoMap} with the input {@link Collection} of {@link Container}s.
   * @param containers collection of {@link Container}s to be added.
   */
  void updateContainers(Collection<Container> containers) {
    for (Container container : containers) {
      addContainer(container.getParentAccountId(), container);
    }
  }

  /**
   * Adds a {@link Container} to the {@link Account}.
   * @param accountId The id of the parent {@link Account} for this {@link Container}.
   * @param container {@link Container} to be added.
   * @throws IllegalArgumentException if {@link Account} with provided id doesn't exist.
   */
  void addContainer(short accountId, Container container) {
    Account parentAccount = idToAccountMap.get(accountId);
    if (parentAccount == null) {
      throw new IllegalArgumentException("Account with ID " + accountId + "doesn't exist");
    }
    AccountBuilder accountBuilder = new AccountBuilder(parentAccount).addOrUpdateContainer(container);
    parentAccount = accountBuilder.build();
    idToAccountMap.put(parentAccount.getId(), parentAccount);
    nameToAccountMap.put(parentAccount.getName(), parentAccount);
  }

  /**
   * Gets {@link Container} by its Parent Account id and id.
   * @param accountId The id of the parent {@link Account} for this {@link Container}.
   * @param id The id to get the {@link Container}.
   * @return The {@link Container} with the given id within the parent Account Id, or {@code null} if
   * such a parent {@link Account} or {@link Container} does not exist.
   */
  Container getContainerByIdForAccount(Short accountId, short id) {
    Account parentAccount = idToAccountMap.get(accountId);
    return parentAccount == null ? null : parentAccount.getContainerById(id);
  }

  /**
   * Gets the last modified time of accounts and containers in this in-memory cache
   * @return the last modified time of accounts and containers
   */
  long getLastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * Sets the last modified time of accounts and containers in this in-memory cache
   * @param lastModifiedTime time when the accounts and containers were last updated
   */
  void setLastModifiedTime(long lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
  }
}
