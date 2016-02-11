import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WagenparkbeheerTest1 {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testIgnition() {
		Wagenparkbeheer w1 = new Wagenparkbeheer();
		assertEquals(0, w1.counter);
		w1.ignition();
		assertEquals(23, w1.counter);
	}

}
