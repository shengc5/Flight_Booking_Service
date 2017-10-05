import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlightService {
	private static final String DBCONFIG_FILENAME = "dbconn.properties";

	public static void usage() {
		/* prints the choices for commands and parameters */
		System.out.println();
		System.out.println(" *** Please enter one of the following commands *** ");
		System.out.println("> login <username> <password>");
		System.out.println("> search <origin_city> <destination_city> <direct> <date> <nb itineraries>");
		System.out.println("> book <itinerary_id>");
		System.out.println("> reservations");
		System.out.println("> cancel <reservation_id>");
		System.out.println("> quit");
	}

	public static String[] tokenize(String command) {
		String regex = "\"([^\"]*)\"|(\\S+)";
		Matcher m = Pattern.compile(regex).matcher(command);
		List<String> tokens = new ArrayList<String>();
		while (m.find()) {
			if (m.group(1) != null) {
				tokens.add(m.group(1));
			} else {
				tokens.add(m.group(2));
			}
		}
		return tokens.toArray(new String[0]);
	}

	public static void menu(Query q) throws Exception {

		/* prepare to read the user's command and parameter(s) */
		String command = null;

		while (true) {
			usage();

			BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
			System.out.print("> ");

			command = r.readLine();
			String[] tokens = tokenize(command.trim());
			if (tokens.length == 0) {
				System.out.println("Please enter a command");
				continue; // back to top of loop
			}

			if (tokens[0].equals("login")) {
				if (tokens.length == 3) {
					/* authenticate the user */
					String username = tokens[1];
					String password = tokens[2];
					q.transaction_login(username,password);
				} else {
					System.out.println("Error: Please provide a username and password");
				}
			}

			else if (tokens[0].equals("search")) {
				/* search for flights */
				if (tokens.length == 6) {
					String originCity = tokens[1];
//					System.out.println(originCity);
					String destinationCity = tokens[2];
//					System.out.println(destinationCity);
					boolean direct = tokens[3].equals("1");
//					System.out.println(direct);
					Integer day;
					Integer count;
					try {
						day = Integer.valueOf(tokens[4]);
						count = Integer.valueOf(tokens[5]);
					} catch (NumberFormatException e) {
						System.out.println("Failed to parse integer");
						continue;
					}
					System.out.println("Searching for flights");
					q.transaction_search_safe(originCity, destinationCity, direct, day, count);
				} else {
					System.out.println("Error: Please provide all search parameters <origin_city> <destination_city> <direct> <date> <nb itineraries>");
				}
			}

			else if (tokens[0].equals("book")) {
				/* book a flight ticket */
				if (tokens.length == 2) {
					int itinerary_id = Integer.parseInt(tokens[1]);
					System.out.println("Booking itinerary.");
					q.transaction_book(itinerary_id);
				} else {
					System.out.println("Error: Please provide an itinerary_id");
				}
			}

			else if (tokens[0].equals("reservations")) {
				/* list all reservations */
				q.transaction_reservations();
			}

			else if (tokens[0].equals("cancel")) {                          
				/* cancel a reservation */
				if (tokens.length == 2) {
					int reservation_id = Integer.parseInt(tokens[1]);
					System.out.println("Canceling reservation.");
					q.transaction_cancel(reservation_id);
				} else {
					System.out.println("Error: Please provide a reservation_id");
				}
			}

			else if (tokens[0].equals("quit")) {
				System.exit(0);
			}
			else {
				System.out.println("Error: unrecognized command '" + tokens[0] + "'");
			}
		}
	}

	public static void main(String[] args) throws Exception {

		/* prepare the database connection stuff */
		Query q = new Query(DBCONFIG_FILENAME);
		q.openConnection();
		q.prepareStatements();
		menu(q); /* menu(...) does the real work */
		q.closeConnection();

	}

}

