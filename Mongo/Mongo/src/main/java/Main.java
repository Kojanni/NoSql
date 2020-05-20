import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class Main {
    public static void main(String[] args) throws IOException {
        JsonWriterSettings writerSettings = new JsonWriterSettings(JsonMode.SHELL, true);
        final String collectionStudents = "students";
        List<Document> jsonStudentsList = new ArrayList<>();
        MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );

        MongoDatabase database = mongoClient.getDatabase("local");

        MongoCollection<Document> students = database.getCollection(collectionStudents);
        students.drop();

        File inputData = new File("src/main/resources/mongo.csv");
        CsvSchema csvSchema = CsvSchema.builder()
                .addColumn("name", CsvSchema.ColumnType.STRING)
                .addColumn("age", CsvSchema.ColumnType.NUMBER)
                .addColumn("courses", CsvSchema.ColumnType.STRING)
                .build().withHeader();
        CsvMapper csvMapper = new CsvMapper();
        List<Object> readAllStudents = csvMapper.readerFor(Map.class).with(csvSchema).readValues(inputData).readAll();
        ObjectMapper mapper = new ObjectMapper();

        readAllStudents.stream().forEach(obj ->{
            try {
                String jsonStudentStr = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
                jsonStudentsList.add(Document.parse(jsonStudentStr));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });
        students.insertMany(jsonStudentsList);

        System.out.println(">Students in DB: " + students.countDocuments());
        System.out.println(">Count of students older 40 years: ");
        students.find(gt("age", "40")).forEach((Consumer<Document>) doc ->{
            System.out.println(doc.toJson(writerSettings));
        });
        System.out.println(">Youngest student: ");
        students.find().sort(ascending("age")).limit(1).forEach((Consumer<Document>) doc ->{
            System.out.println(doc.toJson(writerSettings));
        });


        System.out.println("Courses of oldest student: ");
        students.find().sort(descending("age")).limit(1).forEach((Consumer<Document>) doc ->{
            System.out.println(doc.get("courses"));
        });
    }
}
