package org.linkeddatafragments.views;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.index.IndexDataSource;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

//TODO: Refactor to a composable & flexible architecture using DataSource types, fragments types and request types

/**
 * Serializes an {@link ILinkedDataFragment} to the HTML format
 *
 * @author Miel Vander Sande
 */
public class HtmlTriplePatternFragmentWriterImpl extends
		TriplePatternFragmentWriterBase implements ILinkedDataFragmentWriter {
	private final Configuration cfg;
	private String title;
	private final Template indexTemplate;
	private final Template datasourceTemplate;
	private final Template notfoundTemplate;
	private final Template errorTemplate;

	private final String HYDRA = "http://www.w3.org/ns/hydra/core#";

	/**
	 * @param title
	 *            the title of this service
	 * @param prefixes
	 * @param datasources
	 * @throws IOException
	 */
	public HtmlTriplePatternFragmentWriterImpl(final String title,
			final Map<String, String> prefixes,
			final HashMap<String, IDataSource> datasources) throws IOException {
		super(prefixes, datasources);

		this.cfg = new Configuration(Configuration.VERSION_2_3_22);
		this.cfg.setClassForTemplateLoading(getClass(), "/views");
		this.cfg.setDefaultEncoding("UTF-8");
		this.cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);

		this.indexTemplate = this.cfg.getTemplate("index.ftl.html");
		this.datasourceTemplate = this.cfg.getTemplate("datasource.ftl.html");
		this.notfoundTemplate = this.cfg.getTemplate("notfound.ftl.html");
		this.errorTemplate = this.cfg.getTemplate("error.ftl.html");
		this.title = title;
	}

	/**
	 *
	 * @param outputStream
	 * @param datasource
	 * @param fragment
	 * @param tpfRequest
	 * @throws IOException
	 * @throws TemplateException
	 */
	@Override
	public void writeFragment(final ServletOutputStream outputStream,
			final IDataSource datasource,
			final ITriplePatternFragment fragment,
			final ITriplePatternFragmentRequest tpfRequest) throws IOException,
			TemplateException {
		final Map<String, Object> data = new HashMap<String, Object>();

		// base.ftl.html
		data.put("assetsPath", "assets/");
		data.put("header", this.title);
		data.put("date", new Date());

		// fragment.ftl.html
		data.put("datasourceUrl", tpfRequest.getDatasetURL());
		data.put("datasource", datasource);

		// Parse controls to template variables
		final StmtIterator controls = fragment.getControls();
		while (controls.hasNext()) {
			final Statement control = controls.next();

			final String predicate = control.getPredicate().getURI();
			final RDFNode object = control.getObject();
			if (!object.isAnon()) {
				final String value = object.isURIResource() ? object
						.asResource().getURI() : object.asLiteral()
						.getLexicalForm();
				data.put(predicate.replaceFirst(this.HYDRA, ""), value);
			}
		}

		// Add metadata
		data.put("totalEstimate", fragment.getTotalSize());
		data.put("itemsPerPage", fragment.getMaxPageSize());

		// Add triples and datasources
		final List<Statement> triples = fragment.getTriples().toList();
		data.put("triples", triples);
		data.put("datasources", getDatasources());

		// Calculate start and end triple number
		final long start = ((tpfRequest.getPageNumber() - 1) * fragment
				.getMaxPageSize()) + 1;
		data.put("start", new Long(start));
		final long end = start
				+ (triples.size() < fragment.getMaxPageSize() ? triples.size()
						: fragment.getMaxPageSize()) - 1;
		data.put("end", new Long(end));

		// Compose query object
		final Map<String, String> query = new HashMap<String, String>();
		query.put("subject", getElementAsString(tpfRequest.getSubject()));
		query.put("predicate", getElementAsString(tpfRequest.getPredicate()));
		query.put("object", getElementAsString(tpfRequest.getObject()));
		data.put("query", query);

		// Get the template (uses cache internally)
		final Template temp = datasource instanceof IndexDataSource ? this.indexTemplate
				: this.datasourceTemplate;

		// Merge data-model with template
		temp.process(data, new OutputStreamWriter(outputStream));
	}

	@Override
	public void writeNotFound(final ServletOutputStream outputStream,
			final HttpServletRequest request) throws Exception {
		final Map data = new HashMap();
		data.put("assetsPath", "assets/");
		data.put("datasources", getDatasources());
		data.put("date", new Date());
		data.put("url", request.getRequestURL().toString());

		this.notfoundTemplate.process(data,
				new OutputStreamWriter(outputStream));
	}

	@Override
	public void writeError(final ServletOutputStream outputStream,
			final Exception ex) throws Exception {
		final Map data = new HashMap();
		data.put("assetsPath", "assets/");
		data.put("date", new Date());
		data.put("error", ex);
		this.errorTemplate.process(data, new OutputStreamWriter(outputStream));
	}

	private String getElementAsString(final ITriplePatternElement element) {
		String ret = "";
		if (!element.isVariable()) {
			ret = element.asConstantTerm().toString();
		} else if (element.isAnonymousVariable()) {
			ret = element.asAnonymousVariable().toString();
		}
		return ret;
	}
}
