package com.db2;

public class Transaction {
	public static int TS = 0;
	// public int tid;
	public int trans_timestamp;
	public String trans_state;
	public String items_locked;
	
	
	
	public Transaction() {
	}

	public Transaction(String trans_state, String items_locked) {
		this.trans_state = trans_state;
		this.items_locked = items_locked;
		this.trans_timestamp = ++TS;
	}
	
	public void setItems_locked(String item) {
		if (this.items_locked.equals("none")) {
			this.items_locked = item;
		} else
			this.items_locked = this.items_locked + item;
	}
	
	public String getItems_locked() {

		return this.items_locked;
	}

	public String getTrans_state() {

		return this.trans_state;
	}

	public int getTrans_timestamp() {

		return this.trans_timestamp;
	}

	public void setTrans_state(String state) {

		this.trans_state = state;
	}
}
