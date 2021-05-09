package org.flume;

import java.util.*;
import org.apache.flume.*;

public class MyFlumeSource {
	
	MyFlumeSource(Context context)
	{
		initialize(context);
	}
	
	public void initialize(Context context) {
		System.out.println("TESTTTTEST");
	}	
};
