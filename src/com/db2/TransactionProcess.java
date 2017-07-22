package com.db2;

public class TransactionProcess {

	private String[] filedata = new String[100];
	

/*	public void ReadTransactions() {
		for (int i = 0; i < mains.data.length; i++) {
			filedata[i] = mains.data[i];
		}
		int i = 0;
		while (filedata[i] != null) {
			

			switch (filedata[i].substring(0, 1)) 
			{
			case "b": 
				Transaction transaction = new Transaction("Active", "none");
				int tid = Integer.parseInt(filedata[i].substring(1,
						filedata[i].indexOf(";")));
				mains.transMap.put(tid, transaction);
				System.out.println("Begin Transaction: T" + tid);
				
				break;

			case "r":
				tid = Integer.parseInt(filedata[i].substring(1,
						filedata[i].indexOf("(")));
				LockTable L = new LockTable();
				L.Set_transid_RL(tid);
				String itemname = filedata[i].substring(
						filedata[i].indexOf("(") + 1, filedata[i].indexOf(")"));

				// check if exists in locktable- yes:check read/write lock
				// No:insert into locktable

				if (!mains.lockMap.containsKey(itemname)) {
					mains.lockMap.put(itemname, L);
					System.out.println("T" + tid + " has a read lock on item "
							+ itemname + '\n');
					mains.transMap.get(tid).setItems_locked(itemname);
				} else {
					for (String key : mains.lockMap.keySet()) {

						if (key.equals(itemname)) {
							
							if ((mains.lockMap.get(itemname).Get_transid_WL()) != 0) {
								System.out.println("Item " + key
										+ " is Writelocked and not available!"
										+ '\n');
								// System.out.println("Transaction "+tid+" is blocked");
								// T1.setTrans_state("Blocked");
								enqueue(tid);
								// mains.transMap.put(tid, T1);
								mains.transMap.get(tid).setTrans_state(
										"Blocked");
								// L.Set_trans_waiting_write(tid);
								// mains.lockMap.put(itemname,L);
								mains.lockMap.get(itemname)
										.Set_trans_waiting_write(tid);
							}
							if ((mains.lockMap.get(itemname).Get_transid_RL()) != 0) {
								// L.Set_transid_RL(tid);
								mains.lockMap.get(itemname).Set_transid_RL(tid);
								System.out.println("T" + tid
										+ " has a read lock on item "
										+ itemname + '\n');
								mains.transMap.get(tid).setItems_locked(
										itemname);
							}							
						}

					}
				}

				break;

			case "w":
				String itemname1 = filedata[i].substring(
						filedata[i].indexOf("(") + 1, filedata[i].indexOf(")"));
				tid = Integer.parseInt(filedata[i].substring(1,
						filedata[i].indexOf("(")));
				
				LockTable L1 = new LockTable();
				L1.Set_transid_WL(tid);

				if (!mains.lockMap.containsKey(itemname1)) {
					mains.lockMap.put(itemname1, L1);
					System.out.println("T" + tid + " has a write lock on item "
							+ itemname1 + '\n');
				} else {
					for (String key : mains.lockMap.keySet()) {
						if (key.equals(itemname1)) {
							// for (LockTable value : mains.lockMap.values())
							// {
							// if(value.transid_WL != 0)
							// //No write lock on item
							// {
							if ((mains.lockMap.get(itemname1).Get_transid_WL()) != 0) {
								int timestamp_requesting_trans = mains.transMap
										.get(tid).getTrans_timestamp();
								int timestamp_itemHolding_trans = mains.lockMap
										.get(itemname1).Get_transid_WL();
								// check wait-die and move transaction to
								// queue,change state to block in transaction
								// table
								// add entry in lock table for waiting
								// transaction
								if (timestamp_requesting_trans < timestamp_itemHolding_trans) {
									enqueue(tid);
									mains.transMap.get(tid).setTrans_state(
											"Blocked");
									mains.lockMap.get(itemname1)
											.Set_trans_waiting_write(tid);
								}
							} else {
								if ((mains.lockMap.get(itemname1)
										.Get_transid_RL()) != 0)
								// checking if read lock already exists
								{
									int len_transid_RL = (int) (Math
											.log10(mains.lockMap.get(itemname1)
													.Get_transid_RL()) + 1);
									if (len_transid_RL < 2
											&& mains.lockMap.get(itemname1)
													.Get_transid_RL() == tid) {
										System.out
												.println("T"
														+ tid
														+ " is Upgrading readlock to writelock on item "
														+ itemname1 + '\n');
										mains.lockMap.get(itemname1)
												.replace_transid_RL();
										mains.lockMap.get(itemname1)
												.Set_transid_WL(tid);
										
									}
								}

							}
							// }
						}
					}
				}
				break;

			case "e":
				tid = Integer.parseInt(filedata[i].substring(1, 2));
				if (mains.transMap.get(tid).getTrans_state().equals("Blocked")) {
					System.out.println("Transaction " + tid + " is Committed"
							+ '\n');
				}
				if (mains.transMap.get(tid).getTrans_state().equals("Aborted")) {
					System.out.println("Ignore!Transaction is Aborted");
				}
				if (mains.transMap.get(tid).getTrans_state().equals("Active")) {

					System.out.println("Transaction " + tid + " is Committed"
							+ '\n');
					release(tid);
					// Call release function to release all items held by
					// transaction
				}

				break;
			}
			i++;
		}
	}*/
}
