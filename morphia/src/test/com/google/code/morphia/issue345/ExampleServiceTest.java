package com.google.code.morphia.issue345;

import com.google.code.morphia.AdvancedDatastore;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Version;
import com.google.code.morphia.dao.BasicDAO;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import junit.framework.TestCase;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@RunWith(ConcurrentJunitRunner.class)
@Concurrent(threads = 4)
@Ignore
public class ExampleServiceTest extends TestCase {

	// private ExampleService service = new ExampleService();
	// @Autowired
	BookingDetailService bookingDetailService;

	static MongoConnectionManager mcm;

	static {
		mcm = new MongoConnectionManager();
		MorphiaLoggerFactory.get(ExampleServiceTest.class).debug("starting ...");
	}

	{
		bookingDetailService = new BookingDetailService();
		bookingDetailService.setMongoConnectionManager(mcm);
	}

	@Test
	public void test1() {

		Customer cust = new Customer();
		cust.setName("Jill P");
		cust.setPreferredNumber("0421761183");

		bookingDetailService.book(new Key<BookingDetail>(BookingDetail.class, "24-11-2011"), "09:00 am", cust);
	}

	@Test
	public void test2() {

		Customer cust = new Customer();
		cust.setName("Sam D");
		cust.setPreferredNumber("0421761183");

		bookingDetailService.book(new Key<BookingDetail>(BookingDetail.class, "24-11-2011"), "09:00 am", cust);
	}

	@Test
	public void test3() {

		Customer cust = new Customer();
		cust.setName("Jenny B");
		cust.setPreferredNumber("0421761183");

		bookingDetailService.book(new Key<BookingDetail>(BookingDetail.class, "24-11-2011"), "09:00 am", cust);
	}

	@Test
	public void test4() {

		Customer cust = new Customer();
		cust.setName("Janet T");
		cust.setPreferredNumber("0421761183");

		bookingDetailService.book(new Key<BookingDetail>(BookingDetail.class, "24-11-2011"), "09:00 am", cust);
	}

	@Embedded
	private static class Customer {
		String name;
		String preferredNumber;
		public final String getName() {
			return name;
		}
		public final void setName(String name) {
			this.name = name;
		}
		public final void setPreferredNumber(String preferredNumber) {
			this.preferredNumber = preferredNumber;
		}
	}

	@Embedded
	private static class Consultant {
		private String name;
		public final void setName(String name) {
			this.name = name;
		}
	}

	@Entity("BookingDetail")
	//@Indexes(@Index(unique=true, value="date"))
	private static class BookingDetail {
		@Id private String date;
		@Version private Long version;
		@Embedded private List<BookingSlot> bookingSlot;
		public BookingDetail() {
			super();
			bookingSlot = new ArrayList<BookingSlot>();
		}
		public final void setDate(String date) {
			this.date = date;
		}
		public final Long getVersion() {
			return version;
		}
		public final List<BookingSlot> getBookingSlot() {
			return bookingSlot;
		}
	}

	@Embedded
	private static class BookingSlot {
		// No id because this class is embedded.
		private String startTime;
		private String endTime;
		@Embedded
		private Consultant consultant;
		private boolean enabled;
		@Embedded
		private Customer customer;
		private Date dateCreated;
		public final String getStartTime() {
			return startTime;
		}
		public final void setStartTime(String startTime) {
			this.startTime = startTime;
		}
		public final void setEndTime(String endTime) {
			this.endTime = endTime;
		}
		public final void setConsultant(Consultant consultant) {
			this.consultant = consultant;
		}
		public final void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}
		public final Customer getCustomer() {
			return customer;
		}
		public final void setCustomer(Customer customer) {
			this.customer = customer;
		}
	}

	private static class MongoConnectionManager {
		private final Datastore db;
		public static final String DB_NAME = "cal_dev";
		public MongoConnectionManager() {
			try {
				Mongo m = new Mongo();
				db = new Morphia().map(BookingDetail.class).createDatastore(m, DB_NAME);
				db.ensureIndexes();
			} catch (Exception e) {
				throw new RuntimeException("Error initializing mongo db", e);
			}
		}

		public Datastore getDb() {
			return db;
		}
	}


	private static class BookingDetailService {
		private static final Logger logger = java.util.logging.Logger.getLogger(BookingDetailService.class.getSimpleName());
		MongoConnectionManager mongoConnectionManager;

		public BookingDetail loadOrCreate(Key<BookingDetail> key) {
			BookingDetail bookingDetail = null;
			int tries = 3;
			boolean success = false;
			for (int i = 0; i < tries; i++) {
				try {
					bookingDetail = load(key);
					if (bookingDetail == null) {
						create(key); // Many threads can enter here.
						bookingDetail = load(key);
						if (bookingDetail != null) {
							success = true;
							break;
						}
					} else {
						success = true;
						break;
					}
				} catch (MongoException e) {
					// Duplicate key.
					logger.log( Level.FINE, "loadOrCreate attempt: "+ i +" another user beat us to it.", e);
				}
			}

			if (!success) {
				logger.warning("Could not loadOrCreate at this time, please try again later");
				// TODO change to service exception.
				throw new RuntimeException(
						"Could not loadOrCreate at this time, please try again later");
			}
			return bookingDetail;
		}

		public Key<BookingDetail> create(Key<BookingDetail> key) {
			BookingDetail bookingDetail = new BookingDetail();
			bookingDetail.setDate((String) key.getId());
			createSlots(bookingDetail.getBookingSlot());
			BasicDAO<BookingDetail, ObjectId> dao = new BasicDAO<BookingDetail, ObjectId>(BookingDetail.class, mongoConnectionManager.getDb());
			Key<BookingDetail> result = ((AdvancedDatastore) dao.getDatastore()).insert(bookingDetail);
			dao.ensureIndexes();
			return result;
		}

		public BookingDetail load(Key<BookingDetail> key) {
			BasicDAO<BookingDetail, ObjectId> dao = new BasicDAO<BookingDetail, ObjectId>(BookingDetail.class, mongoConnectionManager.getDb());
			return dao.getDatastore().getByKey(BookingDetail.class, key);

		}

		public Key<BookingDetail> update(BookingDetail bd) {

			BasicDAO<BookingDetail, ObjectId> dao = new BasicDAO<BookingDetail, ObjectId>(
					BookingDetail.class, mongoConnectionManager.getDb());

			Key<BookingDetail> result = dao.getDatastore().save(bd);
			return result;
		}

		public void book(Key<BookingDetail> key, String startTime, Customer customer) {

			int tries = 3;
			boolean success = false;

			for (int i = 0; i < tries; i++) {

				try {

					BookingDetail loadedBookingDetail = loadOrCreate(key);
					List<BookingSlot> availableSlots = new ArrayList<BookingSlot>();
					for (BookingSlot slot : loadedBookingDetail.getBookingSlot()) {

						if (slot.getStartTime().equals(startTime)
								&& slot.getCustomer() == null) {
							availableSlots.add(slot);
						}
					}

					if (availableSlots.size() == 0) {
						// No available slots left, another user must have beaten us to it.
						// TODO change to service exception.
						throw new RuntimeException("No available slots for xxx");
					}

					// TODO Logic to choose consultant.
					availableSlots.get(0).setCustomer(customer);
					logger.log( Level.FINE, "Book for customer: " + customer.getName() + " version: " + loadedBookingDetail.getVersion() + " ...");
					update(loadedBookingDetail);
					logger.log( Level.FINE, "Booked.");

					success = true;
					break;

				} catch (ConcurrentModificationException e) {
					logger.log( Level.FINE, "Book attempt: " + i + " failed, another user beat us to it.", e);
				}

			}

			if (!success) {

				logger.severe("Could not make a booking at this time, please try again later.");

				throw new RuntimeException(
						"Could not make booking at this time, please try again later");
			}
		}
		public final void setMongoConnectionManager(MongoConnectionManager mongoConnectionManager) {
			this.mongoConnectionManager = mongoConnectionManager;
		}

		// Creates two slots.
		private void createSlots(List<BookingSlot> bookingSlots) {

			BookingSlot bookingSlot = new BookingSlot();

			bookingSlot.setEnabled(true);
			bookingSlot.setStartTime("09:00 am");
			bookingSlot.setEndTime("10:00 am");

			Consultant consultant = new Consultant();
			consultant.setName("Peter X");
			bookingSlot.setConsultant(consultant);

			bookingSlots.add(bookingSlot);

			bookingSlot = new BookingSlot();

			bookingSlot.setEnabled(true);
			bookingSlot.setStartTime("09:00 am");
			bookingSlot.setEndTime("10:00 am");

			consultant = new Consultant();
			consultant.setName("Tom X");
			bookingSlot.setConsultant(consultant);

			bookingSlots.add(bookingSlot);

		}

	}

}