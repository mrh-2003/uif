import streamlit.components.v1 as components

def crear_grafo_interactivo(nodes, edges):
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://d3js.org/d3.v7.min.js"></script>
        <style>
            body {{
                margin: 0;
                padding: 0;
                overflow: hidden;
            }}
            #grafo {{
                width: 100vw;
                height: 100vh;
                background: #1a1a1a;
            }}
            .node {{
                cursor: pointer;
                stroke: #fff;
                stroke-width: 2px;
            }}
            .node.persona {{
                fill: #4a9eff;
            }}
            .node.cuenta {{
                fill: #ff9f4a;
            }}
            .node.highlighted {{
                stroke: #ffff00;
                stroke-width: 4px;
            }}
            .link {{
                stroke: #999;
                stroke-opacity: 0.6;
            }}
            .label {{
                font-family: Arial;
                font-size: 12px;
                fill: #fff;
                pointer-events: none;
                text-anchor: middle;
            }}
            .tooltip {{
                position: absolute;
                background: rgba(0, 0, 0, 0.9);
                color: white;
                padding: 10px;
                border-radius: 5px;
                pointer-events: none;
                font-size: 12px;
                display: none;
            }}
            .controls {{
                position: absolute;
                top: 10px;
                right: 10px;
                background: rgba(255, 255, 255, 0.1);
                padding: 15px;
                border-radius: 5px;
            }}
            .search-box {{
                padding: 8px;
                border-radius: 4px;
                border: 1px solid #444;
                background: #2a2a2a;
                color: white;
                margin-bottom: 10px;
                width: 200px;
            }}
            .control-btn {{
                padding: 6px 12px;
                margin: 2px;
                border: none;
                border-radius: 4px;
                background: #4a9eff;
                color: white;
                cursor: pointer;
            }}
            .control-btn:hover {{
                background: #357abd;
            }}
        </style>
    </head>
    <body>
        <div id="grafo"></div>
        <div class="tooltip" id="tooltip"></div>
        <div class="controls">
            <input type="text" class="search-box" id="searchBox" placeholder="Buscar nodo...">
            <button class="control-btn" onclick="resetZoom()">Reset Zoom</button>
            <button class="control-btn" onclick="centerGraph()">Centrar</button>
        </div>
        <script>
            const nodes = {nodes};
            const edges = {edges};
            
            const width = window.innerWidth;
            const height = window.innerHeight;
            
            const svg = d3.select("#grafo")
                .append("svg")
                .attr("width", width)
                .attr("height", height);
            
            const g = svg.append("g");
            
            const zoom = d3.zoom()
                .scaleExtent([0.1, 10])
                .on("zoom", (event) => {{
                    g.attr("transform", event.transform);
                }});
            
            svg.call(zoom);
            
            const simulation = d3.forceSimulation(nodes)
                .force("link", d3.forceLink(edges).id(d => d.id).distance(150))
                .force("charge", d3.forceManyBody().strength(-300))
                .force("center", d3.forceCenter(width / 2, height / 2))
                .force("collision", d3.forceCollide().radius(40));
            
            const link = g.append("g")
                .selectAll("line")
                .data(edges)
                .join("line")
                .attr("class", "link")
                .attr("stroke-width", d => Math.sqrt(d.value) / 100);
            
            const node = g.append("g")
                .selectAll("circle")
                .data(nodes)
                .join("circle")
                .attr("class", d => `node ${{d.tipo}}`)
                .attr("r", d => d.size)
                .call(drag(simulation))
                .on("mouseover", showTooltip)
                .on("mouseout", hideTooltip)
                .on("click", nodeClick);
            
            const label = g.append("g")
                .selectAll("text")
                .data(nodes)
                .join("text")
                .attr("class", "label")
                .text(d => d.label)
                .attr("dy", d => d.size + 15);
            
            simulation.on("tick", () => {{
                link
                    .attr("x1", d => d.source.x)
                    .attr("y1", d => d.source.y)
                    .attr("x2", d => d.target.x)
                    .attr("y2", d => d.target.y);
                
                node
                    .attr("cx", d => d.x)
                    .attr("cy", d => d.y);
                
                label
                    .attr("x", d => d.x)
                    .attr("y", d => d.y);
            }});
            
            function drag(simulation) {{
                function dragstarted(event) {{
                    if (!event.active) simulation.alphaTarget(0.3).restart();
                    event.subject.fx = event.subject.x;
                    event.subject.fy = event.subject.y;
                }}
                
                function dragged(event) {{
                    event.subject.fx = event.x;
                    event.subject.fy = event.y;
                }}
                
                function dragended(event) {{
                    if (!event.active) simulation.alphaTarget(0);
                    event.subject.fx = null;
                    event.subject.fy = null;
                }}
                
                return d3.drag()
                    .on("start", dragstarted)
                    .on("drag", dragged)
                    .on("end", dragended);
            }}
            
            function showTooltip(event, d) {{
                const tooltip = document.getElementById("tooltip");
                tooltip.style.display = "block";
                tooltip.style.left = event.pageX + 10 + "px";
                tooltip.style.top = event.pageY + 10 + "px";
                tooltip.innerHTML = `
                    <strong>ID:</strong> ${{d.id}}<br>
                    <strong>Tipo:</strong> ${{d.tipo}}<br>
                    <strong>Label:</strong> ${{d.label}}
                `;
            }}
            
            function hideTooltip() {{
                document.getElementById("tooltip").style.display = "none";
            }}
            
            function nodeClick(event, d) {{
                node.classed("highlighted", false);
                d3.select(this).classed("highlighted", true);
                
                window.parent.postMessage({{
                    type: 'nodeClick',
                    data: {{
                        id: d.id,
                        label: d.label,
                        tipo: d.tipo
                    }}
                }}, '*');
            }}
            
            function resetZoom() {{
                svg.transition()
                    .duration(750)
                    .call(zoom.transform, d3.zoomIdentity);
            }}
            
            function centerGraph() {{
                const bounds = g.node().getBBox();
                const fullWidth = width;
                const fullHeight = height;
                const midX = bounds.x + bounds.width / 2;
                const midY = bounds.y + bounds.height / 2;
                
                const scale = 0.8 / Math.max(bounds.width / fullWidth, bounds.height / fullHeight);
                const translate = [fullWidth / 2 - scale * midX, fullHeight / 2 - scale * midY];
                
                svg.transition()
                    .duration(750)
                    .call(zoom.transform, d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale));
            }}
            
            document.getElementById("searchBox").addEventListener("input", function(e) {{
                const searchTerm = e.target.value.toLowerCase();
                
                node.classed("highlighted", d => {{
                    return d.id.toLowerCase().includes(searchTerm) || 
                           d.label.toLowerCase().includes(searchTerm);
                }});
            }});
            
            setTimeout(centerGraph, 100);
        </script>
    </body>
    </html>
    """
    
    components.html(html_content, height=800, scrolling=False)
