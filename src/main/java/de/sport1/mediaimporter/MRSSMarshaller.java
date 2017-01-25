//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package de.sport1.mediaimporter;

import com.theplatform.data.api.exception.MidStreamException;
import com.theplatform.data.api.marshalling.*;
import com.theplatform.data.api.marshalling.namespace.Namespace;
import com.theplatform.data.api.objects.Entry;
import com.theplatform.data.api.objects.Feed;
import com.theplatform.media.api.data.objects.Media;
import com.theplatform.media.api.data.objects.MediaFile;
import com.theplatform.module.exception.RuntimeServiceException;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class MRSSMarshaller implements Marshaller {
    private static final String XML_VERSION = "1.0";
    private static XMLOutputFactory writerFactory = XMLOutputFactory.newInstance();
    private Map<String, String> namespaces;

    MRSSMarshaller() {
        namespaces = new TreeMap<>();
        namespaces.put("xsi", "http://www.w3.org/2001/XMLSchema-instance");
        namespaces.put("pl", "http://xml.theplatform.com/data/object");
        namespaces.put("dcterms", "http://purl.org/dc/terms/");
        namespaces.put("media", "http://search.yahoo.com/mrss/");
        namespaces.put("plrelease", "http://xml.theplatform.com/media/data/Release");
        namespaces.put("plfile", "http://xml.theplatform.com/media/data/MediaFile");
        namespaces.put("pla", "http://xml.theplatform.com/data/object/admin");
        namespaces.put("plmedia", "http://xml.theplatform.com/media/data/Media");
        namespaces.put("plingestmf", "http://xml.theplatform.com/ingest/data/IngestMediaFile");
        namespaces.put("plingestm", "http://xml.theplatform.com/ingest/data/IngestMedia");
        namespaces.put("sport1", "http://www.sport1.de/fields");
    }

    public void marshal(Object object, OutputStream output, MarshallingContext context) throws MarshallingException {
        this.marshal(object, output, context, "UTF-8");
    }

    public void marshal(Object object, OutputStream output, MarshallingContext context, String encoding) throws MarshallingException {
        try {
            BufferingOutputStream e = new BufferingOutputStream(output);
            Object xmlWriter = writerFactory.createXMLStreamWriter(e, encoding);
            if (context.isPretty()) {
                xmlWriter = new IndentingXMLStreamWriter((XMLStreamWriter) xmlWriter);
            }

            BufferingXMLStreamWriter writer = new BufferingXMLStreamWriter((XMLStreamWriter) xmlWriter, e);
            writer.writeStartDocument(encoding, "1.0");
            if (object instanceof Feed) {
                this.marshalFeed((Feed) object, writer, context);
            } else {
                if (!(object instanceof Entry)) {
                    throw new IllegalArgumentException("Cannot marshal class of type: " + object.getClass().getName());
                }

                this.marshalEntry((Entry) object, writer, context);
            }

        } catch (XMLStreamException var8) {
            throw new MarshallingException("Could not write to XML stream.", var8);
        } catch (IOException var9) {
            throw new MarshallingException("Could not write to the output stream.", var9);
        }
    }

    private void marshalEntry(Entry entry, BufferingXMLStreamWriter writer, MarshallingContext context) throws MarshallingException, XMLStreamException, IOException {
        try {
            this.startRSS(writer);
            this.writeChannel(entry, writer, context);
            this.endRSS(writer);
        } finally {
            writer.flushBuffer();
        }

    }

    private void marshalFeed(Feed<Entry> feed, BufferingXMLStreamWriter writer, MarshallingContext context) throws MarshallingException, XMLStreamException, IOException {
        this.startRSS(writer);

        try {
            this.writeChannel(feed, writer, context);
        } finally {
            this.endRSS(writer);
            writer.flushBuffer();
        }

    }

    private void startRSS(XMLStreamWriter writer) throws MarshallingException, XMLStreamException {
        writer.writeStartElement("rss");
        writer.writeAttribute("version", "2.0");
        for (Map.Entry<String, String> ns : namespaces.entrySet()) {
            writer.writeNamespace(ns.getKey(), ns.getValue());
        }
    }

    private void writeChannel(Feed<Entry> feed, BufferingXMLStreamWriter writer, MarshallingContext context) throws MarshallingException, XMLStreamException, IOException {
        try {
            writer.writeStartElement("channel");
            this.writeItems(feed.getEntries(), writer, context);
        } finally {
            writer.writeEndElement();
        }

    }

    private void writeChannel(Entry entry, BufferingXMLStreamWriter writer, MarshallingContext context) throws MarshallingException, XMLStreamException, IOException {
        writer.writeStartElement("channel");
        this.writeItems(Collections.singletonList(entry), writer, context);
        writer.writeEndElement();
    }

    private <T extends Entry> void writeItems(List<T> entries, BufferingXMLStreamWriter writer, MarshallingContext context) throws MarshallingException, XMLStreamException, IOException {
        if (entries != null) {
            writer.flushBuffer();

            try {
                for (T e : entries) {
                    writer.flushBuffer();
                    try {
                        writer.writeStartElement("item");
                        writeItem((Media) e, writer, context);
                    } finally {
                        writer.writeEndElement();
                    }
                }

            } catch (Throwable exception) {
                writer.discardBuffer();
                int responseCode = 500;
                if (exception instanceof RuntimeServiceException) {
                    responseCode = ((RuntimeServiceException) exception).getResponseCode();
                }

                this.writeErrorEntry(new ErrorEntry(exception, responseCode, context.getCorrelationId()), writer, context);
                writer.flushBuffer();
                throw new MidStreamException(exception, 200);
            }
        }
    }

    private void writeItem(Media entry, BufferingXMLStreamWriter writer, MarshallingContext context) throws XMLStreamException {
        // this is _nearly_ correct in RSSMarshaller.class
        writer.writeStartElement("guid");
        writer.writeAttribute("isPermalink", "false");
        writer.writeCharacters(entry.getGuid());
        writer.writeEndElement();
        writeElement("title", entry.getTitle(), writer, context);
        writeElement("description", entry.getDescription(), writer, context);
//        if (entry.getExpirationDate() != null) {
//            writeElement("dcterms", "valid", "http://purl.org/dc/terms/", "todo", writer, context);
//        }
        if (entry.getCategoryIds() != null) {
            for (URI uri : entry.getCategoryIds()) {
                writeElement("media", "categoryId", namespaces.get("media"), uri.toString(), writer, context);
            }
        }
//        writeElement("plmedia", "restrictionId", "http://xml.theplatform.com/media/data/Media", "todo", writer, context);
        if (entry.getPubDate() != null) {
            writeElement("pubDate", isoTime(entry.getPubDate()), writer, context);
        }
        if (entry.getContent() != null) {
            writer.writeStartElement("media", "group", "http://search.yahoo.com/mrss/");
            for (MediaFile mf : entry.getContent()) {
                writer.writeStartElement("media", "content", "http://search.yahoo.com/mrss/");
                writeAttribute("filesize", mf.getFileSize(), writer);
                writeAttribute("framerate", mf.getFrameRate(), writer);
                writeAttribute("duration", mf.getDuration(), writer);
                writeAttribute("bitrate", mf.getBitrate(), writer);
                writeAttribute("height", mf.getHeight(), writer);
                writeAttribute("width", mf.getWidth(), writer);
                writeElement("plfile", "aspectRatio", namespaces.get("plingestm"), mf.getAspectRatio().toString(), writer, context);
                writeElement("plfile", "displayAspectRatio", namespaces.get("plingestm"), "16:9", writer, context);
                writer.writeStartElement("plingestmf", "ingestOptions", namespaces.get("plingestmf"));
                writer.writeAttribute("method", "Manage");
                writer.writeEndElement();
                writeElement("plfile", "assetType", namespaces.get("plingestm"), String.join(", ", mf.getAssetTypes()), writer, context);
                writeElement("plfile", "serverId", namespaces.get("plingestm"), mf.getServerId().toString(), writer, context);
                writeElement("plfile", "sourceUrl", namespaces.get("plingestm"), mf.getSourceUrl(), writer, context);
                writer.writeEndElement();
            }
            writer.writeEndElement();
        }
        if (entry.getThumbnails() != null) {
            MediaFile mf = entry.getThumbnails()[0];
            writer.writeStartElement("media", "thumbnail", namespaces.get("media"));
            writeElement("plfile", "assetType", namespaces.get("plingestm"), String.join(", ", mf.getAssetTypes()), writer, context);
            writeElement("plfile", "serverId", namespaces.get("plingestm"), mf.getServerId().toString(), writer, context);
            writeElement("plfile", "sourceUrl", namespaces.get("plingestm"), mf.getSourceUrl(), writer, context);
            writer.writeStartElement("plingestmf", "ingestOptions", namespaces.get("plingestmf"));
            writer.writeAttribute("method", "Copy");
            writer.writeEndElement();
            writer.writeStartElement("plingestmf", "ingestOptions", namespaces.get("plingestmf"));
            writer.writeAttribute("requiredPath", String.format("%s/%s.jpg", entry.getGuid(), entry.getGuid()));
            writer.writeEndElement();
            writer.writeEndElement();
        }
        writer.writeStartElement("plingestm", "workflowOption", namespaces.get("plingestm"));
        writeElement("plingestm", "service", "publish", namespaces.get("plingestm"), writer, context);
        writeElement("plingestm", "method", "publish", namespaces.get("plingestm"), writer, context);
        writer.writeStartElement("plingestm", "argument", namespaces.get("plingestm"));
        writeElement("plingestm", "key", "profile", namespaces.get("plingestm"), writer, context);
        writeElement("plingestm", "value", namespaces.get("plingestm"), "Add releases to progressive MP4 files", writer, context);
        writer.writeEndElement();
        writer.writeEndElement();
        writer.writeStartElement("sport1", "sourceSystemId", namespaces.get("sport1"));
        writer.writeAttribute(namespaces.get("xsi"), "xsi", "type");
        writer.writeCharacters(entry.getGuid());
        writer.writeEndElement();
    }

    private void writeAttribute(String localName, Serializable text, BufferingXMLStreamWriter writer) throws XMLStreamException {
        writer.writeAttribute(localName, text.toString());
    }

    private void writeElement(String prefix, String localName, String namespaceURI, String text, BufferingXMLStreamWriter writer, MarshallingContext context) throws XMLStreamException {
        writer.writeStartElement(prefix, localName, namespaceURI);
        writer.writeCharacters(text);
        writer.writeEndElement();
    }

    private void writeElement(String localName, String text, BufferingXMLStreamWriter writer, MarshallingContext context) throws XMLStreamException {
        writeElement(null, localName, null, text, writer, context);
    }

    private void writeElement(String namespaceURI, String localName, String text, BufferingXMLStreamWriter writer, MarshallingContext context) throws XMLStreamException {
        writeElement(null, localName, namespaceURI, text, writer, context);
    }

    private String isoTime(Date date) {
        TimeZone tz = TimeZone.getTimeZone("GMT");
        DateFormat df = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z"); // @todo which time zone do we get? is it correct to transform here?
        df.setTimeZone(tz);
        return df.format(date);
    }

    private void writeErrorEntry(ErrorEntry entry, XMLStreamWriter writer, MarshallingContext context) throws MarshallingException, XMLStreamException {
        writer.writeStartElement("item");
        writer.writeAttribute("isError", "true");
        writer.writeEndElement();
    }

    private void endRSS(XMLStreamWriter writer) throws MarshallingException, XMLStreamException {
        writer.writeEndElement();
        writer.writeEndDocument();
    }

    private void declareNamespaces(Collection<Namespace> namespaces, XMLStreamWriter writer) throws MarshallingException, XMLStreamException {
        Iterator i$ = namespaces.iterator();

        while (i$.hasNext()) {
            Namespace namespace = (Namespace) i$.next();
            writer.setPrefix(namespace.getPrefix(), namespace.getUri());
            writer.writeNamespace(namespace.getPrefix(), namespace.getUri());
        }

    }
}
