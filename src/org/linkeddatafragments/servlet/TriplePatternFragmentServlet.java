package org.linkeddatafragments.servlet;

import com.google.gson.JsonObject;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.client.utils.URIBuilder;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.TriplePatternFragment;
import org.linkeddatafragments.datasource.IDataSource;

import static org.linkeddatafragments.util.CommonResources.*;

import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.shared.InvalidPropertyURIException;
import org.linkeddatafragments.datasource.DataSourceFactory;

/**
 * Servlet that responds with a Basic Linked Data Fragment.
 *
 * @author Ruben Verborgh
 */
public class TriplePatternFragmentServlet extends HttpServlet {

    private final static long serialVersionUID = 1L;
    private final static Pattern STRINGPATTERN = Pattern.compile("^\"(.*)\"(?:@(.*)|\\^\\^<?([^<>]*)>?)?$");
    private final static TypeMapper types = TypeMapper.getInstance();
    private final static long TRIPLESPERPAGE = 100;

    private ConfigReader config;
    private final HashMap<String, IDataSource> dataSources = new HashMap<>();

    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        try {
            // find the configuration file
            String applicationPathStr = servletConfig.getServletContext().getRealPath("/");
            if (applicationPathStr == null) {	// this can happen when running standalone
                applicationPathStr = System.getProperty("user.dir");
            }
            final File applicationPath = new File(applicationPathStr);

            File configFile = new File(applicationPath, "config-example.json");
            if (servletConfig.getInitParameter("configFile") != null) {
                configFile = new File(servletConfig.getInitParameter("configFile"));
            }

            if (!configFile.exists()) {
                throw new Exception("Configuration file " + configFile + " not found.");
            }

            if (!configFile.isFile()) {
                throw new Exception("Configuration file " + configFile + " is not a file.");
            }

            // load the configuration
            config = new ConfigReader(new FileReader(configFile));
            for (Entry<String, JsonObject> dataSource : config.getDataSources().entrySet()) {
                dataSources.put(dataSource.getKey(), DataSourceFactory.create(dataSource.getValue()));
            }
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
        try {
            // find the data source
            String contextPath = request.getContextPath();
            String requestURI = request.getRequestURI();
            String path = contextPath == null ? requestURI : requestURI.substring(contextPath.length());
            String query = request.getQueryString();
            String dataSourceName = path.substring(1);
            IDataSource dataSource = dataSources.get(dataSourceName);
            if (dataSource == null) {
                throw new Exception("Data source not found.");
            }

            // query the fragment
            Resource subject = parseAsResource(request.getParameter("subject"));
            Property predicate = parseAsProperty(request.getParameter("predicate"));
            RDFNode object = parseAsNode(request.getParameter("object"));
            long page = Math.max(1, parseAsInteger(request.getParameter("page")));
            long limit = TRIPLESPERPAGE, offset = limit * (page - 1);
            TriplePatternFragment fragment = dataSource.getFragment(subject, predicate, object, offset, limit);

            // fill the output model
            Model output = fragment.getTriples();
            output.setNsPrefixes(config.getPrefixes());

            // add dataset metadata
            String hostName = request.getHeader("Host");
            String datasetUrl = request.getScheme() + "://"
                    + (hostName == null ? request.getServerName() : hostName) + request.getRequestURI();
            String fragmentUrl = query == null ? datasetUrl : (datasetUrl + "?" + query);
            Resource datasetId = output.createResource(datasetUrl + "#dataset");
            Resource fragmentId = output.createResource(fragmentUrl);
            
            output.add(datasetId, RDF_TYPE, VOID_DATASET);
            output.add(datasetId, RDF_TYPE, HYDRA_COLLECTION);
            output.add(datasetId, VOID_SUBSET, fragmentId);

            // add fragment metadata
            output.add(fragmentId, RDF_TYPE, HYDRA_COLLECTION);
            output.add(fragmentId, RDF_TYPE, HYDRA_PAGEDCOLLECTION);
            Literal total = output.createTypedLiteral(fragment.getTotalSize(), XSDDatatype.XSDinteger);
            output.add(fragmentId, VOID_TRIPLES, total);
            output.add(fragmentId, HYDRA_TOTALITEMS, total);
            output.add(fragmentId, HYDRA_ITEMSPERPAGE, output.createTypedLiteral(limit, XSDDatatype.XSDinteger));

            // add pages
            final URIBuilder pagedUrl = new URIBuilder(fragmentUrl);
            pagedUrl.setParameter("page", "1");
            output.add(fragmentId, HYDRA_FIRSTPAGE, output.createResource(pagedUrl.toString()));
            if (offset > 0) {
                pagedUrl.setParameter("page", Long.toString(page - 1));
                output.add(fragmentId, HYDRA_PREVIOUSPAGE, output.createResource(pagedUrl.toString()));
            }
            if (offset + limit < fragment.getTotalSize()) {
                pagedUrl.setParameter("page", Long.toString(page + 1));
                output.add(fragmentId, HYDRA_NEXTPAGE, output.createResource(pagedUrl.toString()));
            }

            // add controls
            Resource triplePattern = output.createResource();
            Resource subjectMapping = output.createResource();
            Resource predicateMapping = output.createResource();
            Resource objectMapping = output.createResource();
            
            output.add(datasetId, HYDRA_SEARCH, triplePattern);
            output.add(triplePattern, HYDRA_TEMPLATE, output.createLiteral(datasetUrl + "{?subject,predicate,object}"));
            output.add(triplePattern, HYDRA_MAPPING, subjectMapping);
            output.add(triplePattern, HYDRA_MAPPING, predicateMapping);
            output.add(triplePattern, HYDRA_MAPPING, objectMapping);
            output.add(subjectMapping, HYDRA_VARIABLE, output.createLiteral("subject"));
            output.add(subjectMapping, HYDRA_PROPERTY, RDF_SUBJECT);
            output.add(predicateMapping, HYDRA_VARIABLE, output.createLiteral("predicate"));
            output.add(predicateMapping, HYDRA_PROPERTY, RDF_PREDICATE);
            output.add(objectMapping, HYDRA_VARIABLE, output.createLiteral("object"));
            output.add(objectMapping, HYDRA_PROPERTY, RDF_OBJECT);

            // serialize the output as Turtle
            response.setHeader("Server", "Linked Data Fragments Server");
            response.setContentType("text/turtle");
            response.setCharacterEncoding("utf-8");
            output.write(response.getWriter(), "Turtle", fragmentUrl);
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    /**
     * Parses the given value as an integer.
     *
     * @param value the value
     * @return the parsed value
     */
    private int parseAsInteger(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return 0;
        }
    }

    /**
     * Parses the given value as an RDF resource.
     *
     * @param value the value
     * @return the parsed value, or null if unspecified
     */
    private Resource parseAsResource(String value) {
        RDFNode subject = parseAsNode(value);
        return subject == null || subject instanceof Resource ? (Resource) subject : INVALID_URI;
    }

    /**
     * Parses the given value as an RDF property.
     *
     * @param value the value
     * @return the parsed value, or null if unspecified
     */
    private Property parseAsProperty(String value) {
        RDFNode predicateNode = parseAsNode(value);
        if (predicateNode instanceof Resource) {
            try {
                return ResourceFactory.createProperty(((Resource) predicateNode).getURI());
            } catch (InvalidPropertyURIException ex) {
                return INVALID_URI;
            }
        }
        return predicateNode == null ? null : INVALID_URI;
    }

    /**
     * Parses the given value as an RDF node.
     *
     * @param value the value
     * @return the parsed value, or null if unspecified
     */
    private RDFNode parseAsNode(String value) {
        // nothing or empty indicates an unknown
        if (value == null || value.length() == 0) {
            return null;
        }
        // find the kind of entity based on the first character
        char firstChar = value.charAt(0);
        switch (firstChar) {
            // variable or blank node indicates an unknown
            case '?':
            case '_':
                return null;
            // angular brackets indicate a URI
            case '<':
                return ResourceFactory.createResource(value.substring(1, value.length() - 1));
            // quotes indicate a string
            case '"':
                Matcher matcher = STRINGPATTERN.matcher(value);
                if (matcher.matches()) {
                    String body = matcher.group(1);
                    String lang = matcher.group(2);
                    String type = matcher.group(3);
                    if (lang != null) {
                        return ResourceFactory.createLangLiteral(body, lang);
                    }
                    if (type != null) {
                        return ResourceFactory.createTypedLiteral(body, types.getSafeTypeByName(type));
                    }
                    return ResourceFactory.createPlainLiteral(body);
                }
                return INVALID_URI;
            // assume it's a URI without angular brackets
            default:
                return ResourceFactory.createResource(value);
        }
    }
}