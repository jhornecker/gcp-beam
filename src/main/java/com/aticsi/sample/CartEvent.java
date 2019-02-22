package com.aticsi.sample;

import java.util.UUID;

public class CartEvent {

	public static final String EAN = "EAN";
	public static final String QTE = "QTE";
	public static final String EVENT_ID = "EVENT_ID";

	private String eventID = UUID.randomUUID().toString();

	public void setEventID(String eventID) {
		this.eventID = eventID;
	}

	private final String ean;

	@Override
	public String toString() {
		return "CartEvent [eventID=" + eventID + ", ean=" + ean + ", qte=" + qte + "]";
	}

	private final String qte;

	public CartEvent(String ean, String qte) {
		super();
		this.ean = ean;
		this.qte = qte;
	}

	public String getEan() {
		return ean;
	}

	public String getQte() {
		return qte;
	}

	public String getEventID() {
		return eventID;
	}
}