/**
 * SchedulerJobUtil.java, 16.05.2014
 * $Revision$
 */
package de.axa.bt.zs.common.core.scheduler.impl;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HttpsURLConnection;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.axa.bt.common.util.annotation.BtVersion;
import de.axa.bt.zs.common.core.scheduler.SchedulerException;
import de.axa.bt.zs.common.core.scheduler.SchedulerFactory;
import de.axa.bt.zs.util.BtDiversUtil;

/**
 * Utility class providing several static methods used in the download jobs
 *
 * @author Boris Brauckmann, CodeBox Computerdienste GmbH, for AXA Service AG
 */
@BtVersion(revision = "$Revision$")
public final class SchedulerJobUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerJobUtil.class);

	/**
	 * make sure that no instance of this class is created
	 */
	private SchedulerJobUtil() {
	}

	/**
	 * Sets the last modified attribute of the file to the given time stamp
	 *
	 * @param fullQualifiedFileName the full qualified name of the file
	 * @param lastModified the time stamp to set
	 */
	public static final void updateLastModified(final String fullQualifiedFileName, final long lastModified) {
		if (fullQualifiedFileName == null) {
			return;
		}

		final File file = new File(fullQualifiedFileName);
		if (file.exists() && lastModified >= 0L) {
			file.setLastModified(lastModified);
		}
	}

	/**
	 * @param now current time in milliseconds
	 * @param last the time in milliseconds the job was last run
	 * @param interval the interval in seconds after which the job is considered out-of-date
	 * @return true if the last run is still up-to-date
	 */
	public static final boolean isLastRunUpToDate(final long now, final long last, final long interval) {
		if (last <= 0L) {
			LOGGER.info("never Run: Run now.");
			return false;
		}
		final long outOfDateBoundary = last + interval * 1000L;
		LOGGER.info("Run: Run now? " + now + " " + outOfDateBoundary + " -> " + (now <= outOfDateBoundary));
		return now <= outOfDateBoundary;
	}

	/**
	 * @return the current time in milliseconds with a precision of seconds, i.e. the last three digits are zero
	 */
	public static final long now() {
		final long now = (new Date()).getTime();
		return (now / 1000L) * 1000L;
	}

	/**
	 * Downloads the URL and stores its content in the given list
	 *
	 * @param url the URL to download
	 * @param lines the list of lines
	 * @throws IOException if an I/O-error occurred while downloading and reading the content of the input stream
	 */
	public static final void download(final URL url, final List<String> lines) throws IOException {
		if (url == null || lines == null) {
			return;
		}
		CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));

		final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestProperty("Accept-Charset", "UTF-8");
		connection.setRequestProperty("Accept-Encoding", "gzip");
		connection.connect();
		if (connection instanceof HttpsURLConnection) {
			if (LOGGER.isDebugEnabled()) {
				HttpsURLConnection tHttpsURLConnection = (HttpsURLConnection) connection;
				final String cipherSuite = tHttpsURLConnection.getCipherSuite();
				LOGGER.debug("Connection to  " + url.getHost() + " using " + cipherSuite);
			}
		}

		final int responseCode = connection.getResponseCode();
		switch (responseCode) {
		case HttpURLConnection.HTTP_OK:
			String line = null;
			BufferedReader reader = null;
			try {
				String contentType = connection.getContentType();
				String encoding = connection.getContentEncoding();
				Charset charset = BtDiversUtil.getCharsetFromContentType(contentType);
				if ("gzip".equals(encoding)) {
					reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(connection.getInputStream()), charset));
				} else {
					reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), charset));
				}
				while ((line = reader.readLine()) != null) {
					lines.add(line);
				}
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (final IOException e) {
						// ignore
					}
					reader = null;
				}
			}
			break;
		default:
			throw new IOException("Http-Server returned " + responseCode + " for url " + url.toExternalForm());
		}

	}

	/**
	 * Writes the lines to the file with given file name
	 *
	 * @param lines the lines to write
	 * @param fullQualifiedFileName the full qualified name of the file to which the lines are written
	 * @throws IOException if an I/O-error occurred while writing the file
	 */
	public static final void write(final List<String> lines, final String fullQualifiedFileName) throws IOException {
		if (lines == null || fullQualifiedFileName == null) {
			return;
		}
		checkParentDirectories(fullQualifiedFileName);
		PrintWriter writer = null;
		try {
			final File f = new File(fullQualifiedFileName);
			final FileOutputStream fileOutputStream = new FileOutputStream(f);
			final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, "CP1252");
			writer = new PrintWriter(new BufferedWriter(outputStreamWriter));
			for (final String line : lines) {
				writer.println(line);
			}
		} finally {
			if (writer != null) {
				writer.close();
				writer = null;
			}
		}

	}

	/**
	 * Writes the input stream to the file with given file name
	 *
	 * @param is the input stream which will be written to the file-system
	 * @param fullQualifiedFileName the full qualified name of the file to which the input stream is written
	 * @throws IOException if an I/O-error occurred while writing the file
	 */
	public static final void write(final InputStream is, final String fullQualifiedFileName) throws IOException {
		if (is == null || fullQualifiedFileName == null) {
			return;
		}
		checkParentDirectories(fullQualifiedFileName);
		BufferedOutputStream bos = null;
		try {
			bos = new BufferedOutputStream(new FileOutputStream(fullQualifiedFileName));
			int b;
			while ((b = is.read()) != -1) {
				bos.write(b);
			}
		} finally {
			if (bos != null) {
				try {
					bos.close();
				} catch (final IOException e) {
					// ignore
				}
			}
		}
	}

	/**
	 * creates if necessary the parent directories of the file with given name
	 *
	 * @param fullQualifiedFileName the full qualified file name
	 * @throws IOException if an error occurred while creating the parent directories
	 */
	private static final void checkParentDirectories(final String fullQualifiedFileName) throws IOException {
		if (fullQualifiedFileName == null) {
			return;
		}
		final File file = new File(fullQualifiedFileName);
		final File parentDir = file.getParentFile();
		if (!parentDir.exists()) {
			if (!parentDir.mkdirs()) {
				throw new IOException("Could not create the parent directory of " + fullQualifiedFileName);
			}
		}
	}

	/**
	 * Start a Job with a given Trigger. The Name is used as a job name.
	 *
	 * @param aClass the Job class.
	 * @param aTrigger the trigger.
	 * @param aName the Name.
	 * @return true of ok.
	 */
	public static boolean startJob(final Class<? extends Job> aClass, final Trigger aTrigger, final String aName) {
		try {
			Scheduler scheduler = SchedulerFactory.getInstance().getSchedulerService().getScheduler();
			LOGGER.info("Scheduling Job " + aName + " with class " + aClass + " at " + aTrigger.getStartTime());
			final JobDetail jobDetail = JobBuilder.newJob(aClass).withIdentity(aName, aName + " group").build();
			scheduler.scheduleJob(jobDetail, aTrigger);
			LOGGER.info("Scheduling Job " + aName + " with class " + aClass + " worked");
			return true;
		} catch (org.quartz.SchedulerException | SchedulerException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return false;
	}

	/**
	 * Start a Job at a future date (based on the last character of the hostname * 42 minutes modulo 420) and repeat it
	 * forever every hour.
	 *
	 * @param aClass the class of the Job to start.
	 * @param aName the name of the Job (used for triggergroup and job group.
	 * @return true if it worked.
	 */
	public static boolean startJobHourly(final Class<? extends Job> aClass, final String aName) {
		try {
			Scheduler scheduler = SchedulerFactory.getInstance().getSchedulerService().getScheduler();
			final JobDetail jobDetail = JobBuilder.newJob(aClass).withIdentity(aName, aName + " group").build();
			final Date startDate = futureDate(Calendar.MINUTE, 60 + Math.round(42f) % 60);
			LOGGER.info("Scheduling Job " + aName + " with class " + aClass + " at " + startDate);
			final Trigger trigger = TriggerBuilder.newTrigger().withIdentity(aName + " trigger", aName + " triggerGroup").withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(1).repeatForever()).startAt(startDate).build();
			scheduler.scheduleJob(jobDetail, trigger);
			return true;
		} catch (org.quartz.SchedulerException | SchedulerException e) {
			LOGGER.error(e.getMessage(), e);
		}
		return false;
	}

	private static Date futureDate(final int field, final int amount) {
		final Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MINUTE, amount);
		return calendar.getTime();
	}

}
