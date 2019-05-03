import com.sun.org.apache.xpath.internal.operations.Bool;
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

    private static final String inputFileName = "research.owl";
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

        long start = System.currentTimeMillis();

        System.out.print("Importing papers...");
        processPapers(); System.out.print("Done.\n");

        System.out.print("Importing cited_by (paper, paper)...");
        processCitedBy(); System.out.print("Done.\n");

        System.out.print("Importing keywords...");
        processKeywords(); System.out.print("Done.\n");

        System.out.print("Importing related (keyword,paper)...");
        processRelated(); System.out.print("Done.\n");

        System.out.print("Importing authors...");
        processAuthors(); System.out.print("Done.\n");

        System.out.print("Importing writes and main_author...");
        processWrites(); System.out.print("Done.\n");

        System.out.print("Importing reviews...");
        processReviews(); System.out.print("Done.\n");

        System.out.print("Importing journals...");
        processJournals(); System.out.print("Done.\n");

        System.out.print("Importing conferences...");
        processConferences(); System.out.print("Done.\n");

        System.out.print("Importing editions...");
        processEditions(); System.out.print("Done.\n");

        System.out.print("Importing belongs (edition, conference)...");
        processBelongs(); System.out.print("Done.\n");

        System.out.print("Importing published_in (paper, edition)...");
        processPaperPublishedInEdition(); System.out.print("Done.\n");

        System.out.print("Importing volumes...");
        processVolumes(); System.out.print("Done.\n");

        System.out.print("Importing published_in (paper, volume)...");
        processPaperPublishedInVolume(); System.out.print("Done.\n");

        System.out.print("Importing part_of (volume, journal)...");
        processVolumePartOfJournal(); System.out.print("Done.\n");

        double total = (1.0*System.currentTimeMillis() - start)/60000;
        System.out.println("\nTotal time: " + total + " minutes");

    }

    //TODO: adding pages on published_in (both)

    /**
     * Reads belogns (edition, conference) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processVolumePartOfJournal() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/volumeBelongsJournal.csv"));
        ObjectProperty part_of = base.getObjectProperty(NS + "part_of");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long volume_id = Long.parseLong(tokens[0]);
            long journal_id = Long.parseLong(tokens[1]);

            Resource volume = base.getResource(NS + volume_id);
            Resource journal = base.getResource(NS + journal_id);

            statements.add(ResourceFactory.createStatement(volume, part_of, journal));

        }
        virtModel.add(statements);
    }

    /**
     * Reads edition_published_in (paper, edition) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processPaperPublishedInVolume() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/volume_published_in.csv"));
        ObjectProperty published_in = base.getObjectProperty(NS + "published_in");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long paper_id = Long.parseLong(tokens[0]);
            long volume_id = Long.parseLong(tokens[1]);

            Resource paper = base.getResource(NS + paper_id);
            Resource volume = base.getResource(NS + volume_id);

            statements.add(ResourceFactory.createStatement(paper, published_in, volume));

        }
        virtModel.add(statements);
    }

    /**
     * Reads Editions from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processVolumes() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/volume.csv"));
        OntClass cls = base.getOntClass(NS + "Volume");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long volume_id = Long.parseLong(tokens[0]);
            String vol = tokens[1];
            String title = tokens[2];

            // create Volume properties
            Individual volume = cls.createIndividual(NS + volume_id);

            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            statements.add(base.createStatement(volume, has_title, title_value));

            DatatypeProperty has_vol = base.getDatatypeProperty(NS + "vol");
            Literal vol_value = base.createTypedLiteral(vol, XSDDatatype.XSDstring);
            statements.add(base.createStatement(volume, has_vol, vol_value));

        }
        virtModel.add(statements);
    }

    /**
     * Reads edition_published_in (paper, edition) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processPaperPublishedInEdition() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/edition_published_in.csv"));
        ObjectProperty published_in = base.getObjectProperty(NS + "published_in");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long paper_id = Long.parseLong(tokens[0]);
            long edition_id = Long.parseLong(tokens[1]);

            Resource paper = base.getResource(NS + paper_id);
            Resource edition = base.getResource(NS + edition_id);

            statements.add(ResourceFactory.createStatement(paper, published_in, edition));

        }
        virtModel.add(statements);
    }

    /**
     * Reads belogns (edition, conference) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processBelongs() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/belongs.csv"));
        ObjectProperty belongs_to = base.getObjectProperty(NS + "belongs_to");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long edition_id = Long.parseLong(tokens[0]);
            long conference_id = Long.parseLong(tokens[1]);

            Resource edition = base.getResource(NS + edition_id);
            Resource conference = base.getResource(NS + conference_id);

            statements.add(ResourceFactory.createStatement(edition, belongs_to, conference));

        }
        virtModel.add(statements);
    }

    /**
     * Reads Editions from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processEditions() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/edition.csv"));
        OntClass cls = base.getOntClass(NS + "Edition");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long edition_id = Long.parseLong(tokens[0]);
            String title = tokens[1];
            String venue = tokens[2];
            String city = tokens[3]; //skipping 4: period
            String year = tokens[5];

            // create Edition properties
            Individual edition = cls.createIndividual(NS + edition_id);

            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            statements.add(base.createStatement(edition, has_title, title_value));

            DatatypeProperty has_venue = base.getDatatypeProperty(NS + "venue");
            Literal venue_value = base.createTypedLiteral(venue, XSDDatatype.XSDstring);
            statements.add(base.createStatement(edition, has_venue, venue_value));

            DatatypeProperty has_city = base.getDatatypeProperty(NS + "city");
            Literal city_value = base.createTypedLiteral(city, XSDDatatype.XSDstring);
            statements.add(base.createStatement(edition, has_city, city_value));

            DatatypeProperty has_year = base.getDatatypeProperty(NS + "year");
            Literal year_value = base.createTypedLiteral(year, XSDDatatype.XSDint);
            statements.add(base.createStatement(edition, has_year, year_value));
        }
        virtModel.add(statements);
    }

    /**
     * Reads cited_by (paper, paper) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processCitedBy() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/cited_by_year.csv"));
        ObjectProperty citedBy = base.getObjectProperty(NS + "cited_by");

        String line = br.readLine(); //remove header: Author_ID - Paper_ID
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long paper1_id = Long.parseLong(tokens[0]);
            long paper2_id = Long.parseLong(tokens[1]);

            Resource originPaper = base.getResource(NS + paper1_id);
            Resource citedByPaper = base.getResource(NS + paper2_id);

            statements.add(ResourceFactory.createStatement(originPaper, citedBy, citedByPaper));
        }

        virtModel.add(statements);
    }

    /**
     * Reads conferences from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processConferences() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/conferences.csv"));
        OntClass cls = base.getOntClass(NS + "Conference");
        List<String> conf_types = getSubclasses(cls);

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            int random = (int) (Math.random() * conf_types.size());
            String[] tokens = line.split(";");
            long journal_id = Long.parseLong(tokens[0]);
            String title = tokens[1];
            String conf_type = conf_types.get(random);

            // create Conference properties
            Individual conference = base.getOntClass(NS + conf_type).createIndividual(NS + journal_id);
            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            statements.add(base.createStatement(conference, has_title, title_value));

        }
        virtModel.add(statements);
    }

    /**
     * Reads journals from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processJournals() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/journal.csv"));
        OntClass cls = base.getOntClass(NS + "Journal");
        List<String> journal_types = getSubclasses(cls);

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            int random = (int) (Math.random() * journal_types.size());
            String[] tokens = line.split(";");
            long journal_id = Long.parseLong(tokens[0]);
            String title = tokens[1];
            String journal_type = journal_types.get(random);

            // create Journal properties
            Individual journal = base.getOntClass(NS + journal_type).createIndividual(NS + journal_id);
            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            statements.add(base.createStatement(journal, has_title, title_value));

        }
        virtModel.add(statements);
    }

    /**
     * Reads reviews (author, reviews, paper) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processReviews() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/reviews.csv"));
        ObjectProperty reviews = base.getObjectProperty(NS + "reviews");

        String line = br.readLine(); //remove header: Author_ID - Paper_ID
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long author_id = Long.parseLong(tokens[0]);
            long paper_id = Long.parseLong(tokens[1]);

            Resource author = base.getResource(NS + author_id);
            Resource paper = base.getResource(NS + paper_id);

            statements.add(ResourceFactory.createStatement(author, reviews, paper));

        }
        virtModel.add(statements);
    }

    /**
     * Reads writes (author, paper, isMainAuthor) from csv file and inserts triplet instances into virtuoso.
     *  - author, writes, paper AND if main_author is true:
     *  - author, main_author, paper
     *
     * @throws IOException
     */
    private static void processWrites() throws IOException {
        List<Statement> statements = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader("input/writes.csv"));
        ObjectProperty writes = base.getObjectProperty(NS + "writes");
        ObjectProperty main_author = base.getObjectProperty(NS + "main_author");

        String line = br.readLine(); //remove header: Author_ID - Paper_ID - Main_author (True/False)
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long author_id = Long.parseLong(tokens[0]);
            long paper_id = Long.parseLong(tokens[1]);
            boolean isMainAuthor = Boolean.parseBoolean(tokens[2]);

            Resource author = base.getResource(NS + author_id);
            Resource paper = base.getResource(NS + paper_id);

            statements.add(ResourceFactory.createStatement(author, writes, paper));

            if(isMainAuthor){
                statements.add(ResourceFactory.createStatement(author, main_author, paper));
            }
        }
        virtModel.add(statements);
    }

    /**
     * Reads Authors from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processAuthors() throws IOException {
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
    private static void processRelated() throws IOException {
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
    private static void processKeywords() throws IOException {
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
    private static void processPapers() throws IOException {
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
