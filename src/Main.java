import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.query.*;
import virtuoso.jena.driver.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Main {

    private static final String inputFileName = "SDMlab3.owl";
    private static final String SOURCE = "http://www.semanticweb.org/saradiaz/ontologies/2019/3/SDMlab3";
    private static final String NS = SOURCE + "#";

    private static OntModel base;
    private static OntModel inf;

    private static VirtGraph virtGraph;
    private static VirtModel virtModel;


    public static void main(String[] args) throws IOException {
        InputStream in = FileManager.get().open(inputFileName); //locate input OWL file
        base = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM); //create the base model
        base.read(in, "RDF/XML"); //read owl file of RDF/XML type
        //inf = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MICRO_RULE_INF, base); //create inference model

        virtGraph = new VirtGraph("http://localhost:8890/research", "jdbc:virtuoso://jynx.fib.upc.es:1111", "dba", "dba");
        virtGraph.clear();
        virtModel = new VirtModel(virtGraph);

        System.out.println("Importing papers...");
        readPapers();
        System.out.println("Papers imported.");

        System.out.println("Importing keywords...");
        readKeywords();
        System.out.println("Keywords imported.");

        System.out.println("Importing related (keyword,paper)...");
        readRelated();
        System.out.println("Related imported.");

        System.out.println("Importing authors...");
        readAuthors();
        System.out.println("Authors imported.");

    }

    /**
     * Reads Authors from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void readAuthors() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/author.csv"));
        OntClass cls = base.getOntClass(NS + "Author");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long author_id = Long.parseLong(tokens[0]);
            String author_name = tokens[1];

            // create Author properties
            Individual author = cls.createIndividual(NS + author_id);

            DatatypeProperty has_name = base.getDatatypeProperty(NS + "name");
            Literal name_value = base.createTypedLiteral(author_name, XSDDatatype.XSDstring);
            statements.add(base.createStatement(author, has_name, name_value));
        }
        virtModel.add(statements);
    }

    /**
     * Reads related from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void readRelated() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/related.csv"));
        ObjectProperty related = base.getObjectProperty(NS + "related");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long kw_id = Long.parseLong(tokens[0]);
            long paper_id = Long.parseLong(tokens[1]);

            Resource kw = base.getResource(NS + kw_id);
            Resource paper = base.getResource(NS + paper_id);

            statements.add(ResourceFactory.createStatement(kw, related, paper));
        }
        virtModel.add(statements);
    }

    /**
     * Reads keywords from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void readKeywords() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/keywords.csv"));
        OntClass cls = base.getOntClass(NS + "Keyword");
        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long kw_id = Long.parseLong(tokens[0]);
            String kw_name = tokens[1];

            // create keyword properties
            Individual keyword = cls.createIndividual(NS + kw_id);
            DatatypeProperty has_kw_name = base.getDatatypeProperty(NS + "keyword_name");
            Literal kw_value = base.createTypedLiteral(kw_name, XSDDatatype.XSDstring);
            statements.add(base.createStatement(keyword, has_kw_name, kw_value));
        }
        virtModel.add(statements);
    }

    /**
     * Reads papers from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void readPapers() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/article.csv"));
        OntClass cls = base.getOntClass(NS + "Paper");
        List<String> paper_types = getSubclasses(cls);

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            int random = (int) (Math.random() * paper_types.size());
            String[] tokens = line.split(";");
            long paper_id = Long.parseLong(tokens[0]);
            String doi = tokens[1];
            String title = tokens[3];

            String paper_type = paper_types.get(random);

            // create Paper properties
            Individual paper = base.getOntClass(NS + paper_type).createIndividual(NS + paper_id);

            DatatypeProperty has_doi = base.getDatatypeProperty(NS + "doi");
            Literal doi_value = base.createTypedLiteral(doi, XSDDatatype.XSDstring);
            statements.add(base.createStatement(paper, has_doi, doi_value));

            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            statements.add(base.createStatement(paper, has_title, title_value));

        }
        virtModel.add(statements);
    }

    /**
     * Gets all the subclasses of a class and returns it as list of strings.
     *
     * @param cls
     * @return
     */
    private static List<String> getSubclasses(OntClass cls) {
        List<String> types = new ArrayList<>();
        for (Iterator i = cls.listSubClasses(true); i.hasNext(); ) {
            OntClass c = (OntClass) i.next();
            //System.out.print("\t " + c.getLocalName() + "\n");
            types.add(c.getLocalName());
        }
        return types;
    }

    /**
     * Prints the ontology schema.
     *
     * @param m
     */
    public static void print(OntModel m) {
        System.out.println("=== Printing information regarding the model ===");
        ExtendedIterator classes = m.listClasses();
        while (classes.hasNext()) {
            OntClass cls = (OntClass) classes.next();

            System.out.println("Classes: " + cls.getLocalName());
            for (Iterator i = cls.listSubClasses(true); i.hasNext(); ) {
                OntClass c = (OntClass) i.next();
                System.out.print(" " + c.getLocalName() + "\n");
            }
        }
    }

    /**
     * Queries all triplets of the research graph
     */
    private static void query() {
        Query sparql = QueryFactory.create("SELECT * FROM <http://localhost:8890/research> WHERE { ?s <http://localhost:8890/research> ?o }");
        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(sparql, virtGraph);
        ResultSet results = vqe.execSelect();
        System.out.println("\nSELECT results:");
        while (results.hasNext()) {
            QuerySolution rs = results.nextSolution();
            RDFNode s = rs.get("s");
            RDFNode p = rs.get("p");
            RDFNode o = rs.get("o");
            System.out.println(" { " + s + " " + p + " " + o + " . }");
        }
        System.out.println("virtGraph.getCount() = " + virtGraph.getCount());
    }

}
