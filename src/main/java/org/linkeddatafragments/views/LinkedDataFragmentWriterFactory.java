package org.linkeddatafragments.views;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.linkeddatafragments.datasource.IDataSource;

/**
 * A factory for {@link ILinkedDataFragmentWriter}s.
 *
 * @author Miel Vander Sande
 */
public class LinkedDataFragmentWriterFactory {

	private final static String HTML = "text/html";

	/**
	 * Creates {@link ILinkedDataFragmentWriter} for a given mimeType
	 * 
	 * @param prefixes
	 *            Configured prefixes to be used in serialization
	 * @param datasources
	 *            Configured datasources
	 * @param mimeType
	 *            mimeType to create writer for
	 * @return created writer
	 * @throws IOException
	 */
	public static ILinkedDataFragmentWriter create(final String title,
			final Map<String, String> prefixes,
			final HashMap<String, IDataSource> datasources,
			final String mimeType) throws IOException {
		switch (mimeType) {
		case HTML:
			return new HtmlTriplePatternFragmentWriterImpl(title, prefixes,
					datasources);
		default:
			return new RdfWriterImpl(prefixes, datasources, mimeType);
		}
	}
}
