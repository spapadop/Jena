//import com.sun.org.apache.xpath.internal.operations.Bool;
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

    //TODO: connect with dbpedia
    //TODO: adding pages on published_in (both)

    public static void main(String[] args) throws IOException {
        InputStream in = FileManager.get().open(inputFileName); //locate input OWL file
        base = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MICRO_RULE_INF); //create the base model
        base.read(in, "RDF/XML"); //read owl file of RDF/XML type

        virtGraph = new VirtGraph("http://localhost:8890/research", "jdbc:virtuoso://jynx.fib.upc.es:1111", "dba", "dba");
        virtGraph.clear();
        virtModel = new VirtModel(virtGraph);

        long start = System.currentTimeMillis();

        System.out.print("Inserting papers...");
        processPapers(); System.out.print("Done.\n");

        System.out.print("Inserting cited_by (paper, paper)...");
        processCitedBy(); System.out.print("Done.\n");

        System.out.print("Inserting keywords...");
        processKeywords(); System.out.print("Done.\n");

        System.out.print("Inserting related (keyword,paper)...");
        processRelated(); System.out.print("Done.\n");

        System.out.print("Inserting authors...");
        processAuthors(); System.out.print("Done.\n");

        System.out.print("Inserting writes and main_author...");
        processWrites(); System.out.print("Done.\n");

        System.out.print("Inserting reviews...");
        processReviews(); System.out.print("Done.\n");

        System.out.print("Inserting journals...");
        processJournals(); System.out.print("Done.\n");

        System.out.print("Inserting conferences...");
        processConferences(); System.out.print("Done.\n");

        System.out.print("Inserting editions...");
        processEditions(); System.out.print("Done.\n");

        System.out.print("Inserting belongs (edition, conference)...");
        processBelongs(); System.out.print("Done.\n");

        System.out.print("Inserting published_in (paper, edition)...");
        processPaperPublishedInEdition(); System.out.print("Done.\n");

        System.out.print("Inserting volumes...");
        processVolumes(); System.out.print("Done.\n");

        System.out.print("Inserting published_in (paper, volume)...");
        processPaperPublishedInVolume(); System.out.print("Done.\n");

        System.out.print("Inserting part_of (volume, journal)...");
        processVolumePartOfJournal(); System.out.print("Done.\n");

        System.out.print("Importing TBOX + ABOX into Virtuoso...");
        base.write(new PrintWriter("tbox_abox.owl")); //write to file
        virtModel.add(base.getBaseModel()); //write to Virtuoso
        System.out.print("Done.\n");

        double total = (1.0 * System.currentTimeMillis() - start) / 60000;
        System.out.println("\nTotal time: " + total + " minutes");

    }

    /**
     * Reads part_of (volume, journal) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processVolumePartOfJournal() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/volumeBelongsJournal.csv"));
        ObjectProperty part_of = base.getObjectProperty(NS + "part_of");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long volume_id = Long.parseLong(tokens[0]);
            long journal_id = Long.parseLong(tokens[1]);

            Resource volume = base.getResource(NS + volume_id);
            Resource journal = base.getResource(NS + journal_id);

            base.add(volume, part_of, journal);
        }
    }

    /**
     * Reads edition_published_in (paper, edition) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processPaperPublishedInVolume() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/volume_published_in.csv"));
        ObjectProperty published_in = base.getObjectProperty(NS + "published_in");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long paper_id = Long.parseLong(tokens[0]);
            long volume_id = Long.parseLong(tokens[1]);

            OntResource paper = base.getOntResource(NS + paper_id);
            String paper_type = paper.getRDFType().getLocalName(); //Full_Paper, Short_Paper, Demo_Paper, Survey_Paper
            try {
                if ("Full_Paper".equals(paper_type) || "Short_Paper".equals(paper_type)) {
                    Resource volume = base.getResource(NS + volume_id);
                    base.add(paper, published_in, volume);
                }
            } catch (NullPointerException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * Reads Editions from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processVolumes() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/volume.csv"));
        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long volume_id = Long.parseLong(tokens[0]);
            String vol = tokens[1];
            String title = tokens[2];

            // create Volume properties
            Individual volume = base.getOntClass(NS + "Volume").createIndividual(NS + volume_id);

            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            base.add(volume, has_title, title_value);

            DatatypeProperty has_vol = base.getDatatypeProperty(NS + "vol");
            Literal vol_value = base.createTypedLiteral(vol, XSDDatatype.XSDstring);
            base.add(volume, has_vol, vol_value);
        }
    }

    /**
     * Reads edition_published_in (paper, edition) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processPaperPublishedInEdition() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/edition_published_in.csv"));
        ObjectProperty published_in = base.getObjectProperty(NS + "published_in");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long paper_id = Long.parseLong(tokens[0]);
            long edition_id = Long.parseLong(tokens[1]);

            OntResource paper = base.getOntResource(NS + paper_id);
            String paper_type = paper.getRDFType().getLocalName(); //Full_Paper, Short_Paper, Demo_Paper, Survey_Paper
            try {
                if ("Full_Paper".equals(paper_type) || "Short_Paper".equals(paper_type)) {
                    Resource edition = base.getResource(NS + edition_id);
                    base.add(paper, published_in, edition);
                }
            } catch (NullPointerException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * Reads belogns (edition, conference) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processBelongs() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/belongs.csv"));
        ObjectProperty belongs_to = base.getObjectProperty(NS + "belongs_to");

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long edition_id = Long.parseLong(tokens[0]);
            long conference_id = Long.parseLong(tokens[1]);

            Resource edition = base.getResource(NS + edition_id);
            Resource conference = base.getResource(NS + conference_id);
            base.add(edition, belongs_to, conference);
        }
    }

    /**
     * Reads Editions from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processEditions() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/edition.csv"));

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long edition_id = Long.parseLong(tokens[0]);
            String title = tokens[1];
            String venue = tokens[2];
            String city = tokens[3]; //skipping 4: period
            String year = tokens[5];

            // create Edition properties
            Individual edition = base.getOntClass(NS + "Edition").createIndividual(NS + edition_id);

            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            base.add(edition, has_title, title_value);

            DatatypeProperty has_venue = base.getDatatypeProperty(NS + "venue");
            Literal venue_value = base.createTypedLiteral(venue, XSDDatatype.XSDstring);
            base.add(edition, has_venue, venue_value);

            DatatypeProperty has_city = base.getDatatypeProperty(NS + "city");
            Literal city_value = base.createTypedLiteral(city, XSDDatatype.XSDstring);
            base.add(edition, has_city, city_value);

            DatatypeProperty has_year = base.getDatatypeProperty(NS + "year");
            Literal year_value = base.createTypedLiteral(year, XSDDatatype.XSDint);
            base.add(edition, has_year, year_value);
        }
    }

    /**
     * Reads cited_by (paper, paper) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processCitedBy() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/cited_by_year.csv"));
        ObjectProperty citedBy = base.getObjectProperty(NS + "cited_by");

        String line = br.readLine(); //remove header: Author_ID - Paper_ID
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long paper1_id = Long.parseLong(tokens[0]);
            long paper2_id = Long.parseLong(tokens[1]);

            OntResource paper1 = base.getOntResource(NS + paper1_id);
            OntResource paper2 = base.getOntResource(NS + paper2_id);
            String paper_type1 = paper1.getRDFType().getLocalName(); //Full_Paper, Short_Paper, Demo_Paper, Survey_Paper
            String paper_type2 = paper2.getRDFType().getLocalName(); //Full_Paper, Short_Paper, Demo_Paper, Survey_Paper

            if (("Full_Paper".equals(paper_type1) || "Short_Paper".equals(paper_type1))
                    && ("Full_Paper".equals(paper_type2) || "Short_Paper".equals(paper_type2))) {
                base.add(paper1, citedBy, paper2);
            }
        }
    }

    /**
     * Reads conferences from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processConferences() throws IOException {
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
            base.add(conference, has_title, title_value);
        }
    }

    /**
     * Reads journals from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processJournals() throws IOException {
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
            base.add(journal, has_title, title_value);
        }
    }

    /**
     * Reads reviews (author, reviews, paper) from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processReviews() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/reviews.csv"));
        ObjectProperty reviews = base.getObjectProperty(NS + "reviews");

        String line = br.readLine(); //remove header: Author_ID - Paper_ID
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long author_id = Long.parseLong(tokens[0]);
            long paper_id = Long.parseLong(tokens[1]);
            OntResource paper = base.getOntResource(NS + paper_id);
            String paper_type = paper.getRDFType().getLocalName(); //Full_Paper, Short_Paper, Demo_Paper, Survey_Paper
            try {
                if ("Full_Paper".equals(paper_type) || "Short_Paper".equals(paper_type)) {
                    Resource author = base.getResource(NS + author_id);
                    base.add(author, reviews, paper);
                }
            } catch (NullPointerException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    /**
     * Reads writes (author, paper, isMainAuthor) from csv file and inserts triplet instances into virtuoso.
     * - author, writes, paper AND if main_author is true:
     * - author, main_author, paper
     *
     * @throws IOException
     */
    private static void processWrites() throws IOException {
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

            base.add(author, writes, paper);

            if (isMainAuthor) {
                base.add(author, main_author, paper);
            }
        }
    }

    /**
     * Reads Authors from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processAuthors() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/author.csv"));
        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long author_id = Long.parseLong(tokens[0]);
            String author_name = tokens[1];

            // create Author properties
            Individual author = base.getOntClass(NS + "Author").createIndividual(NS + author_id);

            DatatypeProperty has_name = base.getDatatypeProperty(NS + "name");
            Literal name_value = base.createTypedLiteral(author_name, XSDDatatype.XSDstring);
            base.add(author, has_name, name_value);
        }
    }

    /**
     * Reads related from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processRelated() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/related.csv"));
        ObjectProperty related = base.getObjectProperty(NS + "related");
        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long kw_id = Long.parseLong(tokens[0]);
            long paper_id = Long.parseLong(tokens[1]);

            Resource kw = base.getResource(NS + kw_id);
            Resource paper = base.getResource(NS + paper_id);

            base.add(kw, related, paper);
        }
    }

    /**
     * Reads keywords from csv file and inserts triplet instances into virtuoso.
     *
     * @throws IOException
     */
    private static void processKeywords() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("input/keywords.csv"));
        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long kw_id = Long.parseLong(tokens[0]);
            String kw_name = tokens[1];

            // create keyword properties
            Individual keyword = base.getOntClass(NS + "Keyword").createIndividual(NS + kw_id);
            DatatypeProperty has_kw_name = base.getDatatypeProperty(NS + "keyword_name");
            Literal kw_value = base.createTypedLiteral(kw_name, XSDDatatype.XSDstring);
            base.add(keyword, has_kw_name, kw_value);
        }
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
            base.add(paper, has_doi, doi_value);

            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            base.add(paper, has_title, title_value);
        }
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
