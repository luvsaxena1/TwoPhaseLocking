package com.db2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransactionProcess {

	private String[] filedata = new String[100];

	public void readTransactions() {
		for (int i = 0; i < mains.data.length; i++) {
			filedata[i] = mains.data[i];
		}
		int i = 0;
		while (filedata[i] != null) {
			System.out.println("Operation: " + filedata[i]);
			switch (filedata[i].substring(0, 1)) {
			case "b":
				Transaction transaction = new Transaction("Active");
				int tid = Integer.parseInt(filedata[i].substring(1, filedata[i].indexOf(";")));
				mains.transMap.put(tid, transaction);
				System.out.println("Begin Transaction: T" + tid);
				break;

			case "r":
				tid = Integer.parseInt(filedata[i].substring(1, filedata[i].indexOf("(")));
				if (mains.transMap.get(tid).getTrans_state() != "Aborted") {
					LockTable L = new LockTable();
					List<Integer> readList = null;
					List<String> lockItemList = null;
					String itemname = filedata[i].substring(filedata[i].indexOf("(") + 1, filedata[i].indexOf(")"));

					// check if exists in locktable- yes:check read/write lock
					// No:insert into locktable

					if (!mains.lockMap.containsKey(itemname)) {
						readList = new ArrayList<Integer>();
						lockItemList = new ArrayList<String>();
						readList.add(tid);
						L.setTransid_RL(readList);
						mains.lockMap.put(itemname, L);
						if (mains.transMap.get(tid).getItems_locked() != null) {
							lockItemList = mains.transMap.get(tid).getItems_locked();
						}
						lockItemList.add(itemname);
						mains.transMap.get(tid).setItems_locked(lockItemList);
						System.out.println("T" + tid + " has a read lock on item " + itemname);
					} else {
						for (String key : mains.lockMap.keySet()) {

							if (key.equals(itemname)) {
								if (mains.lockMap.get(itemname).getTransid_RL() != null) {
									List<Integer> existingReadList = mains.lockMap.get(itemname).getTransid_RL();
									existingReadList.add(tid);
									mains.lockMap.get(itemname).setTransid_RL(existingReadList);
									lockItemList = mains.transMap.get(tid).getItems_locked();
									lockItemList.add(itemname);
									mains.transMap.get(tid).setItems_locked(lockItemList);
									System.out.println("T" + tid + " has a read lock on item " + itemname);
								}
								
								else if ((mains.lockMap.get(itemname).getTransid_WL()) != 0) {
									System.out.println("Item " + key + " is Writelocked and not available!");
									check_deadlock(tid, itemname, "r");
								}
								else {
									readList = new ArrayList<Integer>();
									readList.add(tid);
									mains.lockMap.get(itemname).setTransid_RL(readList);
									if(mains.transMap.get(tid).getItems_locked() != null){
										lockItemList = mains.transMap.get(tid).getItems_locked();
									}
									lockItemList.add(itemname);
									mains.transMap.get(tid).setItems_locked(lockItemList);
									System.out.println("T" + tid + " has a read lock on item " + itemname);
								}
							}
						}
					}
				} else
					System.out.println("Operation r" + tid + " could not be performed as transaction " + tid
							+ " is already aborted!");
				break;

			case "w":
				tid = Integer.parseInt(filedata[i].substring(1, filedata[i].indexOf("(")));
				if (mains.transMap.get(tid).getTrans_state() != "Aborted") {
					List<String> lockItemList = null;
					LockTable L1 = new LockTable();
					String itemname1 = filedata[i].substring(filedata[i].indexOf("(") + 1, filedata[i].indexOf(")"));
					// check if exists in locktable- yes:check read/write lock
					// No:insert into locktable
					if (!mains.lockMap.containsKey(itemname1)) {
						lockItemList = new ArrayList<String>();
						L1.setTransid_WL(tid);
						mains.lockMap.put(itemname1, L1);
						if (mains.transMap.get(tid).getItems_locked() != null) {
							lockItemList = mains.transMap.get(tid).getItems_locked();
						}
						lockItemList.add(itemname1);
						mains.transMap.get(tid).setItems_locked(lockItemList);
						System.out.println("T" + tid + " has a write lock on item " + itemname1);
					} else {
						for (String key : mains.lockMap.keySet()) {
							if (key.equals(itemname1)) {
								if ((mains.lockMap.get(itemname1).getTransid_WL() != 0)) {
									System.out.println("Item " + key
											+ " is Writelocked and not available! Proccessing for wait and die condition");
									// Check for wait and die condition
									check_deadlock(tid, itemname1, "w");
								}
								List<Integer> readList1 = mains.lockMap.get(itemname1).getTransid_RL();
								if ((readList1.size() == 1) && readList1.get(0) == tid) {
									upgradeReadToWrite(tid, itemname1);
								} else {
									// check wait and die here again put it in a
									// waiting list . so once all
									// the read list item unlock it. It will get
									// the lock
									check_deadlock(tid, itemname1, "w");
								}
							}
						}
					}
				} else
					System.out.println("Operation w" + tid + " could not be performed as transaction " + tid
							+ " is already aborted!");
				break;

			case "e":
				tid = Integer.parseInt(filedata[i].substring(1, filedata[i].indexOf(";")));
				if (mains.transMap.get(tid).getTrans_state() != "Aborted") {
					mains.transMap.get(tid).setTrans_state("Committed");
					System.out.println("Transaction " + tid + " has committed");
					unlockTransaction(tid);
				} else
					System.out.println("Operation e" + tid + " could not be performed as transaction " + tid
							+ " is already aborted!");
				break;
			}
			i++;
		}
	}

	/**
	 * @param tid
	 * @param itemname1
	 */
	private void upgradeReadToWrite(int tid, String itemname1) {
		List<Integer> readList1;
		readList1 = null;
		mains.lockMap.get(itemname1).setTransid_RL(readList1);
		// making read list empty before upgrading
		// the lock
		mains.lockMap.get(itemname1).setTransid_WL(tid);
		System.out.println("T" + tid + " has upgraded to write lock from read Lock on item " + itemname1);
	}

	public void unlockTransaction(Integer tid) {
		if (!mains.transMap.get(tid).getItems_locked().isEmpty()) {
			List<String> lockedItemList = mains.transMap.get(tid).getItems_locked();
			/**
			 * Loop to iterate over item and see if they have any other lock if
			 * they have lock on different transaction id then remove the lock
			 * of the transaction which is either aborting or committing
			 */
			if (lockedItemList != null) {
				for (String itemName : lockedItemList) {
					if (mains.lockMap.get(itemName).getTransid_RL() != null) {
						List<Integer> transidRL = mains.lockMap.get(itemName).getTransid_RL();
						int i = 0;
						if(!transidRL.isEmpty()){
						for (Integer commitAbortTid : transidRL) {
							if (commitAbortTid == tid) {
								transidRL.remove(i);
							}
							i++;
						}
						}	
						mains.lockMap.get(itemName).setTransid_RL(transidRL);
					} else {
						if (mains.lockMap.get(itemName).getTransid_WL() != 0) {
							Integer transidWL = mains.lockMap.get(itemName).getTransid_WL();
							if (transidWL == tid) {
								transidWL = 0;
							}
							mains.lockMap.get(itemName).setTransid_WL(transidWL);
						}
					}
					// REFINING WAIT LIST
					if (!mains.waitTransactionList.isEmpty()) {
						int i = 0;
						for (String waitTransaction : mains.waitTransactionList) {
							Integer removedTransaction = Integer.parseInt(waitTransaction.substring(1, 2));
							if (removedTransaction == tid) {
								mains.waitTransactionList.remove(i);
							}
							i++;
						}
					}
					if (!mains.waitTransactionList.isEmpty()) {
						// Refining the waitTransactionList
						String waitingTransaction = mains.waitTransactionList.get(0);
						Integer readWaitListId = Integer.parseInt(waitingTransaction.substring(1, 2));
						String itemName1 = waitingTransaction.substring(2, 3);
						Integer writeLockTid = mains.lockMap.get(itemName).getTransid_WL();
						List<Integer> readList1 = null;
						readList1 = mains.lockMap.get(itemName).getTransid_RL();
						if (waitingTransaction.substring(0, 1).equals("w")) {
							if (readList1 != null) {
								if (readList1.size() == 1 && readList1.contains(readWaitListId)
										&& itemName1.equals(itemName)) {
									upgradeReadToWrite(readWaitListId, itemName1);
								} else {
									System.out.println("Transaction " + waitingTransaction.substring(0, 2)
											+ " keeps waiting in the wait list");
								}
							}
							if (readList1 == null || writeLockTid == 0 && itemName1.equals(itemName)) {
								System.out.println("Assign from wait list");
								writeLockTid = readWaitListId;
								mains.lockMap.get(itemName).setTransid_WL(writeLockTid);
								mains.waitTransactionList.remove(0);
								System.out.println("Assigning lock to operation w"+writeLockTid+" from waiting list on item "+itemName1);
							}
							if (readWaitListId != writeLockTid) {
								System.out.println("Transaction " + waitingTransaction.substring(0, 2)
										+ " keeps waiting in the wait list");
							}
						}

						else if (waitingTransaction.substring(0, 1).equals("r")) {
							if (writeLockTid == 0 && itemName1.equals(itemName)) {

								if (readList1 == null) {
									readList1 = new ArrayList<Integer>();
								}
								readList1.add(readWaitListId);
								mains.lockMap.get(itemName).setTransid_RL(readList1);
								mains.waitTransactionList.remove(0);
								System.out.println("Assigning lock to operation r"+writeLockTid+" from waiting list on item "+itemName1);
							} else {
								System.out.println("Transaction " + waitingTransaction.substring(0, 2)
										+ " keeps waiting in the wait list");
							}
						}
					}
				}
			}
			// making locked item list empty of transaction committed or aborted
			lockedItemList = null;
			mains.transMap.get(tid).setItems_locked(lockedItemList);
		}
	}

	private void check_deadlock(int tid, String itemname1, String oper) {
		int timestamp_requesting_trans = mains.transMap.get(tid).getTrans_timestamp();
		int transid_itemHolding_trans = 0, timestamp_itemHolding_trans = 0;
		List<Integer> read_transid = mains.lockMap.get(itemname1).getTransid_RL();

		// check if item is readlocked or writelocked by a transaction and
		// retrieve that transaction id
		if (mains.lockMap.get(itemname1).getTransid_WL() != 0) {
			transid_itemHolding_trans = mains.lockMap.get(itemname1).getTransid_WL();
		} else {
			transid_itemHolding_trans = Collections.min(read_transid);
		}
		timestamp_itemHolding_trans = mains.transMap.get(transid_itemHolding_trans).getTrans_timestamp();
		if (timestamp_requesting_trans <= timestamp_itemHolding_trans) {
			mains.transMap.get(tid).setTrans_state("Blocked");
			if (transid_itemHolding_trans == tid && oper.equals("w")) {
				mains.waitTransactionList.addLast(oper + tid + itemname1);
			} else if (mains.lockMap.get(itemname1).getTransid_WL() == tid) {
				mains.waitTransactionList.addLast(oper + tid + itemname1);
			}
			System.out.println(oper + tid + " is waiting for item " + itemname1);
		} else {
			mains.transMap.get(tid).setTrans_state("Aborted");
			System.out.println("Transaction T" + tid + " is aborted");
			unlockTransaction(tid);
		}
		return;
	}
}
