public class Test {

    public static void main(String[] args) throws Exception {
        KalturaToMpxImporter importer = new KalturaToMpxImporter();
//        CategoryImporter.etl(importer);
        MediaImporter.etl(importer);

//        converter.showSomeMpxMedia(converter.getMpxMediaClient());
    }

}
