package org.linkeddatafragments.servlet;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpHeaders;
import org.apache.jena.riot.Lang;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.DataSourceTypesRegistry;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IDataSourceType;
import org.linkeddatafragments.datasource.index.IndexDataSource;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.util.MIMEParse;
import org.linkeddatafragments.views.ILinkedDataFragmentWriter;
import org.linkeddatafragments.views.LinkedDataFragmentWriterFactory;

import com.google.gson.JsonObject;

/**
 * Servlet that responds with a Linked Data Fragment.
 *
 * @author Ruben Verborgh
 * @author Bart Hanssens
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class LinkedDataFragmentServlet extends HttpServlet {

	private final static long serialVersionUID = 1L;

	// Parameters

	/**
     *
     */
	public final static String CFGFILE = "configFile";

	private ConfigReader config;
	private final HashMap<String, IDataSource> dataSources = new HashMap<>();
	private final Collection<String> mimeTypes = new ArrayList<>();

	private File getConfigFile(final ServletConfig config) throws IOException {
		String path = config.getServletContext().getRealPath("/");
		if (path == null) {
			// this can happen when running standalone
			path = System.getProperty("user.dir");
		}
		File cfg = new File(path, "config-example.json");
		if (config.getInitParameter(CFGFILE) != null) {
			cfg = new File(config.getInitParameter(CFGFILE));
		}
		if (!cfg.exists()) {
			throw new IOException("Configuration file " + cfg + " not found.");
		}
		if (!cfg.isFile()) {
			throw new IOException("Configuration file " + cfg
					+ " is not a file.");
		}
		return cfg;
	}

	/**
	 *
	 * @param servletConfig
	 * @throws ServletException
	 */
	@Override
	public void init(final ServletConfig servletConfig) throws ServletException {
		try {
			// load the configuration
			final File configFile = getConfigFile(servletConfig);
			this.config = new ConfigReader(new FileReader(configFile));

			// register data source types
			for (final Entry<String, IDataSourceType> typeEntry : this.config
					.getDataSourceTypes().entrySet()) {
				DataSourceTypesRegistry.register(typeEntry.getKey(),
						typeEntry.getValue());
			}

			// register data sources
			for (final Entry<String, JsonObject> dataSource : this.config
					.getDataSources().entrySet()) {
				this.dataSources.put(dataSource.getKey(),
						DataSourceFactory.create(dataSource.getValue()));
			}

			// register content types
			MIMEParse.register("text/html");
			MIMEParse.register(Lang.RDFXML.getHeaderString());
			MIMEParse.register(Lang.NTRIPLES.getHeaderString());
			MIMEParse.register(Lang.JSONLD.getHeaderString());
			MIMEParse.register(Lang.TTL.getHeaderString());
		} catch (final Exception e) {
			throw new ServletException(e);
		}
	}

	/**
     *
     */
	@Override
	public void destroy() {
		for (final IDataSource dataSource : this.dataSources.values()) {
			try {
				dataSource.close();
			} catch (final Exception e) {
				// ignore
			}
		}
	}

	/**
	 * Get the datasource
	 *
	 * @param request
	 * @return
	 * @throws IOException
	 */
	private IDataSource getDataSource(final HttpServletRequest request)
			throws DataSourceNotFoundException {
		final String contextPath = request.getContextPath();
		final String requestURI = request.getRequestURI();

		final String path = contextPath == null ? requestURI : requestURI
				.substring(contextPath.length());

		if (path.equals("/") || path.isEmpty()) {
			final String baseURL = FragmentRequestParserBase.extractBaseURL(
					request, this.config);
			return new IndexDataSource(baseURL, this.dataSources);
		}

		final String dataSourceName = path.substring(1);
		final IDataSource dataSource = this.dataSources.get(dataSourceName);
		if (dataSource == null) {
			throw new DataSourceNotFoundException(dataSourceName);
		}
		return dataSource;
	}

	/**
	 *
	 * @param request
	 * @param response
	 * @throws ServletException
	 */
	@Override
	public void doGet(final HttpServletRequest request,
			final HttpServletResponse response) throws ServletException {
		// if a request is for the context path without a trailing slash,
		// we redirect to the context path plus trailing slash
		final String contextPath = request.getServletContext().getContextPath();
		if (!contextPath.isEmpty()
				&& request.getRequestURI().endsWith(contextPath)) {
			response.setStatus(301);
			final String locationHeader = request.getServletContext()
					.getContextPath() + "/";
			response.setHeader("Location", locationHeader);
		} else {
			ILinkedDataFragment fragment = null;
			try {
				// do conneg
				final String acceptHeader = request
						.getHeader(HttpHeaders.ACCEPT);
				final String bestMatch = MIMEParse.bestMatch(acceptHeader);

				// set additional response headers
				response.setHeader(HttpHeaders.SERVER,
						"Linked Data Fragments Server");
				response.setHeader("Access-Control-Allow-Origin", "*");
				response.setContentType(bestMatch);
				response.setCharacterEncoding(StandardCharsets.UTF_8.name());

				// create a writer depending on the best matching mimeType
				final ILinkedDataFragmentWriter writer = LinkedDataFragmentWriterFactory
						.create(this.config.getTitle(),
								this.config.getPrefixes(), this.dataSources,
								bestMatch);

				try {

					final IDataSource dataSource = getDataSource(request);

					final ILinkedDataFragmentRequest ldfRequest = dataSource
							.getRequestParser().parseIntoFragmentRequest(
									request, this.config);

					fragment = dataSource.getRequestProcessor()
							.createRequestedFragment(ldfRequest);
					writer.writeFragment(response.getOutputStream(),
							dataSource, fragment, ldfRequest);

				} catch (final DataSourceNotFoundException ex) {
					try {
						response.setStatus(404);
						writer.writeNotFound(response.getOutputStream(),
								request);
					} catch (final Exception ex1) {
						throw new ServletException(ex1);
					}
				} catch (final Exception e) {
					e.printStackTrace();
					response.setStatus(500);
					writer.writeError(response.getOutputStream(), e);
				}

			} catch (final Exception e) {
				e.printStackTrace();
				throw new ServletException(e);
			} finally {
				// close the fragment
				if (fragment != null) {
					try {
						fragment.close();
					} catch (final Exception e) {
						// ignore
					}
				}
			}
		}
	}
}
