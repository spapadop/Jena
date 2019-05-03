import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.XSD;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Main {

    private static final String inputFileName = "SDMlab3.owl";
    private static final String SOURCE = "http://www.semanticweb.org/saradiaz/ontologies/2019/3/SDMlab3";
    private static final String NS = SOURCE + "#";

    private static OntModel base;
    private static OntModel inf;


    public static void main(String[] args) throws IOException {
        InputStream in = FileManager.get().open(inputFileName); //locate input OWL file
        base = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM); //create the base model
        base.read(in, "RDF/XML"); //read owl file of RDF/XML type
        inf = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MICRO_RULE_INF, base); //create inference model

        readPapers("input/article.csv");
        readKeywords("input/keywords.csv");


        // Create datatype property 'hasAge'
//        DatatypeProperty hasAge = ontModel.createDatatypeProperty(ns + "hasAge");
//        // 'hasAge' takes integer values, so its range is 'integer'
//        // Basic datatypes are defined in the â€vocabularyâ€™ package
//        hasAge.setDomain(person);
//        hasAge.setRange(XSD.integer); // com.hp.hpl.jena.vocabulary.XSD
//
//        // Create individuals
//        Individual john = malePerson.createIndividual(ns + "John");
//        Individual jane = femalePerson.createIndividual(ns + "Jane");
//        Individual bob = malePerson.createIndividual(ns + "Bob");
//
//        // Create statement 'John hasAge 20'
//        Literal age20 = ontModel.createTypedLiteral("20", XSDDatatype.XSDint);
//        Statement johnIs20 = ontModel.createStatement(john, hasAge, age20);
//        ontModel.add(johnIs20);


        // list the asserted types
//        for (Iterator<Resource> i = p1.listRDFTypes(false); i.hasNext(); ) {
//            System.out.println( p1.getURI() + " is asserted in class " + i.next() );
//        }

        // list the inferred types
//        p1 = inf.getIndividual( NS + "paper1" );
//        for (Iterator<Resource> i = p1.listRDFTypes(false); i.hasNext(); ) {
//            System.out.println( p1.getURI() + " is inferred to be in class " + i.next() );
//        }
    }



    private static void readKeywords(String filepath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filepath));
        OntClass cls = base.getOntClass(NS + "Keyword");
        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(";");
            long kw_id = Long.parseLong(tokens[0]);
            String kw_name = tokens[1];

            // create Paper properties
            Individual keyword = cls.createIndividual(NS + kw_id);
            DatatypeProperty has_kw_name = base.getDatatypeProperty(NS + "keyword_name");
            Literal kw_value = base.createTypedLiteral(kw_name, XSDDatatype.XSDstring);
            Statement st = base.createStatement(keyword,has_kw_name,kw_value);

            System.out.println("------Keyword-----");
            System.out.println(st.toString());
            System.out.println("------------------");
        }
    }


    private static void readPapers(String filepath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filepath));
        OntClass cls = base.getOntClass(NS + "Paper");
        List<String> paper_types = getSubclasses(cls);

        String line = br.readLine(); //remove header
        while ((line = br.readLine()) != null) {
            int random = (int) (Math.random()* paper_types.size());
            String[] tokens = line.split(";");
            long paper_id = Long.parseLong(tokens[0]);
            String doi = tokens[1];
            String title = tokens[3];

            String paper_type = paper_types.get(random);

            // create Paper properties
            Individual paper = base.getOntClass(NS + paper_type).createIndividual(NS + paper_id);

            DatatypeProperty has_doi = base.getDatatypeProperty(NS + "doi");
            Literal doi_value = base.createTypedLiteral(doi, XSDDatatype.XSDstring);
            Statement st = base.createStatement(paper,has_doi,doi_value);

            DatatypeProperty has_title = base.getDatatypeProperty(NS + "title");
            Literal title_value = base.createTypedLiteral(title, XSDDatatype.XSDstring);
            Statement st2 = base.createStatement(paper,has_title,title_value);

            System.out.println("------Paper------");
            System.out.println(st.toString());
            System.out.println(st2.toString());
            System.out.println("------------------");


            // Create individuals
//        Individual john = malePerson.createIndividual(ns + "John");
//        Individual jane = femalePerson.createIndividual(ns + "Jane");
//        Individual bob = malePerson.createIndividual(ns + "Bob");
//
//        // Create statement 'John hasAge 20'
//        Literal age20 = ontModel.createTypedLiteral("20", XSDDatatype.XSDint);
//        Statement johnIs20 = ontModel.createStatement(john, hasAge, age20);
//        ontModel.add(johnIs20);


//            OntClass paperType = model.getOntClass(NS + papers[random]); //select random class from 4 available: Full, Short, Demo, Survey
//            String[] tokens = line.split(";");
//            Individual p1 = model.createIndividual( NS + tokens[0], model.getOntClass( NS + paperType ));
//            Property writes = base.getProperty(NS + "writes");
//            Property reviews = base.getProperty(NS + "reviews");
//            DatatypeProperty paper_id = base.getDatatypeProperty(NS + "paper_id");
            //p1.setPropertyValue();
        }
    }

//    private static void createStringStatement(Object sub_id, String theClass, String obj){
//        Individual subject = base.getOntClass(NS + theClass).createIndividual(NS + sub_id.toString());
//        DatatypeProperty has_rel = base.getDatatypeProperty(NS + obj);
//        Literal value = base.createTypedLiteral(obj, XSDDatatype.XSDstring);
//        Statement st = base.createStatement(subject,has_rel,value);
//        System.out.println(st.toString());
//    }

    private static List<String> getSubclasses(OntClass cls) {
        List<String> types = new ArrayList<>();
        for (Iterator i = cls.listSubClasses(true); i.hasNext(); ) {
            OntClass c = (OntClass) i.next();
            //System.out.print("\t " + c.getLocalName() + "\n");
            types.add(c.getLocalName());
        }
        return types;
    }


    public static void print(OntModel m){
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
}
