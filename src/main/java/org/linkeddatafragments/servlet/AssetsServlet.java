package org.linkeddatafragments.servlet;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.ext.com.google.common.io.Files;

public class AssetsServlet extends HttpServlet {

	private Map<String, String> mimeTypeMapping = new HashMap<String, String>();

	@Override
	public void init(final ServletConfig config) throws ServletException {
		super.init(config);
		this.mimeTypeMapping.put("jpg", "image/jpeg");
		this.mimeTypeMapping.put("gif", "image/gif");
		this.mimeTypeMapping.put("ico", "image/ico");
		this.mimeTypeMapping.put("css", "text/css");
		this.mimeTypeMapping.put("html", "text/html");
	};

	@Override
	public void doGet(final HttpServletRequest request,
			final HttpServletResponse response) throws ServletException,
			IOException {
		try {

			final URL resource = super.getServletContext().getResource(
					request.getServletPath() + request.getPathInfo());
			if (resource == null) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND);
			} else {
				final File f = new File(resource.toURI());
				response.setStatus(HttpServletResponse.SC_OK);
				response.setContentLengthLong(f.length());
				response.setContentType(this.mimeTypeMapping.get(f.getName()
						.substring(f.getName().lastIndexOf('.'))));
				final ServletOutputStream os = response.getOutputStream();
				Files.copy(f, os);
				os.flush();
				os.close();
			}
		} catch (final MalformedURLException x) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
		} catch (final URISyntaxException x) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
		}
	}
}
