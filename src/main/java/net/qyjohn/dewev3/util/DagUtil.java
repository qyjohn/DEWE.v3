package net.qyjohn.dewev3.util;

import java.io.*;
import java.util.*;

import org.dom4j.*;
import org.dom4j.io.*;
import org.apache.log4j.Logger;


public class DagUtil
{
	public SAXReader reader;
	public Document document;
	public List<Element> jobs, children;
	final static Logger logger = Logger.getLogger(DagUtil.class);

	
	/**
	 *
	 * Constructor
	 *
	 */
	 
	public DagUtil(String filename)
	{
		try
		{
			reader = new SAXReader();
			document = reader.read(new File(filename));
			jobs = document.getRootElement().elements("job");
			children = document.getRootElement().elements("child");
		} catch (Exception e)
		{
			System.out.println(e.getMessage());	
			e.printStackTrace();
		}
	}

	public void generateGexf(String filename)
	{
		try
		{
			Document gexf = DocumentHelper.createDocument();
			int edgeId = 0;
		
			// Create the root element
			Element root = gexf.addElement( "gexf" );
			root.addAttribute("xmlns", "http://www.gexf.net/1.2draft");
			root.addAttribute("hello", "world");
			root.addAttribute("version", "1.2");
		
			// Create the graph, nodes, edges element
			Element graph = root.addElement("graph").addAttribute("mode", "static"). addAttribute("defaultedgetype", "directed");
			Element nodes = graph.addElement("nodes");
			Element edges = graph.addElement("edges");
			
			// Populate the nodes
			for (Element e : jobs)
			{
				nodes.addElement("node").addAttribute("id", e.attributeValue("id")).addAttribute("label", e.attributeValue("name"));
			}

			// Populate the edges
			for (Element e : children)
			{
				String target = e.attributeValue("ref");
				List<Element> parents = e.elements("parent");
				for (Element parent: parents)
				{
					String source = parent.attributeValue("ref");
					String id = String.format("ID%06d", edgeId);
					edges.addElement("edge").addAttribute("id", id).addAttribute("source", source).addAttribute("target", target);	
					edgeId++;	
				}
			}
			
			// Create pretty XML
			FileOutputStream out = new FileOutputStream(new File(filename));
			OutputFormat format = OutputFormat.createPrettyPrint();
			XMLWriter writer = new XMLWriter(out, format);
			writer.write(gexf);
			writer.close();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());	
			e.printStackTrace();
		}
	}

	public void generateD3(String filename, String graph)
	{
		try
		{
			Document gexf = DocumentHelper.createDocument();
			LinkedList<String> nodes = new LinkedList<String>();
			LinkedList<String> edges = new LinkedList<String>();

			// Populate the nodes
			for (Element e : jobs)
			{
				String id = e.attributeValue("id");
				String name = e.attributeValue("name");
				String item = String.format("{id: '%s', value: {label: '%s'}}", id, name);
				nodes.add(item);
			}

			// Populate the edges
			int edgeId = 0;
			for (Element e : children)
			{
				String target = e.attributeValue("ref");
				List<Element> parents = e.elements("parent");
				for (Element parent: parents)
				{
					String source = parent.attributeValue("ref");
					String id = String.format("ID%06d", edgeId);
					String item = String.format("{u: '%s', v: '%s'}", source, target);
//					String item = String.format("{u: '%s', v: '%s', value: {label: '%s'}}", source, target, id);
					edges.add(item);
					edgeId++;	
				}
			}
			
			// Create pretty XML
			FileWriter fw = new FileWriter(filename);
			fw.write("loadData(\n");
			fw.write("{\n");
			fw.write("name: 'graph1',\n");
			fw.write("nodes: [\n");
			for (int i=0; i<nodes.size()-1; i++)
			{
				fw.write(nodes.get(i) + ",\n");
			}
			fw.write(nodes.get(nodes.size()-1) + "\n");
			fw.write("],\nlinks:[\n");
			for (int i=0; i<edges.size()-1; i++)
			{
				fw.write(edges.get(i) + ",\n");
			}
			fw.write(edges.get(edges.size()-1) + "\n");
			fw.write("]\n}\n);");
			fw.close();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());	
			e.printStackTrace();
		}
	}

	public static void main(String[] args)
	{
		DagUtil du = new DagUtil(args[0]);
		du.generateD3(args[1], "test");
	}

}


