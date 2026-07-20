/**
 * 3D Network Graph renderer for Malla (ForceGraph3D / Three.js).
 *
 * Provides a spin-able 3D view of traceroute topology and emits
 * directional particles when packets / traceroute hops are observed.
 */
(function (global) {
  "use strict";

  let graph = null;
  let containerEl = null;
  let autoRotate = true;
  let rotateHandle = null;
  let linkIndex = new Map(); // "a-b" -> link object
  let nodeById = new Map();
  let lastGraphData = null;

  function linkKey(a, b) {
    return [Number(a), Number(b)].sort((x, y) => x - y).join("-");
  }

  function normalizeLinks(links) {
    return (links || []).map((link) => {
      const source =
        typeof link.source === "object" ? link.source.id : link.source;
      const target =
        typeof link.target === "object" ? link.target.id : link.target;
      return {
        ...link,
        source: Number(source),
        target: Number(target),
      };
    });
  }

  function rebuildIndexes(data) {
    linkIndex = new Map();
    nodeById = new Map();
    (data.nodes || []).forEach((n) => nodeById.set(Number(n.id), n));
    (data.links || []).forEach((link) => {
      const source =
        typeof link.source === "object" ? link.source.id : link.source;
      const target =
        typeof link.target === "object" ? link.target.id : link.target;
      linkIndex.set(linkKey(source, target), link);
    });
  }

  function nodeColor(node) {
    if (node.__flashUntil && Date.now() < node.__flashUntil) {
      return "#ffc107";
    }
    const connections = node.connections || 0;
    if (connections >= 8) return "#0d6efd";
    if (connections >= 4) return "#198754";
    if (connections >= 2) return "#6f42c1";
    return "#6c757d";
  }

  function linkColor(link) {
    if (link.__flashUntil && Date.now() < link.__flashUntil) {
      return "#ffc107";
    }
    if (link.type === "indirect") return "rgba(255,152,0,0.35)";
    const snr = link.avg_snr;
    if (snr == null) return "rgba(148,163,184,0.55)";
    if (snr >= 0) return "rgba(25,135,84,0.75)";
    if (snr >= -10) return "rgba(13,110,253,0.7)";
    return "rgba(220,53,69,0.65)";
  }

  function stopAutoRotate() {
    if (rotateHandle) {
      cancelAnimationFrame(rotateHandle);
      rotateHandle = null;
    }
    if (graph) {
      const controls = graph.controls();
      if (controls) {
        controls.autoRotate = false;
      }
    }
  }

  function startAutoRotate() {
    stopAutoRotate();
    if (!graph || !autoRotate) return;
    const controls = graph.controls();
    if (!controls) return;
    controls.autoRotate = true;
    controls.autoRotateSpeed = 1.1;
    // Keep the render loop aware of auto-rotate updates
    const tick = () => {
      if (!graph || !autoRotate) return;
      if (typeof controls.update === "function") {
        controls.update();
      }
      rotateHandle = requestAnimationFrame(tick);
    };
    rotateHandle = requestAnimationFrame(tick);
  }

  function ensureGraph() {
    if (graph) return graph;
    if (typeof ForceGraph3D !== "function") {
      console.error("ForceGraph3D is not loaded");
      return null;
    }
    containerEl = document.getElementById("networkGraph");
    if (!containerEl) return null;

    containerEl.innerHTML = "";
    graph = ForceGraph3D()(containerEl)
      .backgroundColor(
        getComputedStyle(document.body).getPropertyValue("--bs-body-bg") ||
          "#0b1220"
      )
      .showNavInfo(false)
      .nodeId("id")
      .nodeLabel((n) => {
        const heard = n.last_seen
          ? new Date(n.last_seen * 1000).toLocaleString()
          : "unknown";
        return `${n.name || n.id}<br/>links: ${n.connections || 0}<br/>last: ${heard}`;
      })
      .nodeVal((n) => Math.max(2, Number(n.size) || 4))
      .nodeColor(nodeColor)
      .nodeOpacity(0.95)
      .linkSource("source")
      .linkTarget("target")
      .linkColor(linkColor)
      .linkWidth((l) =>
        l.__flashUntil && Date.now() < l.__flashUntil
          ? 2.8
          : Math.max(0.6, Math.min(2.4, (l.strength || 1) * 0.8))
      )
      .linkOpacity(0.85)
      .linkDirectionalParticles(0)
      .linkDirectionalParticleWidth(3.5)
      .linkDirectionalParticleSpeed(0.008)
      .linkDirectionalParticleColor(() => "#ffe066")
      .onNodeClick((node) => {
        if (typeof global.selectGraphNode === "function") {
          global.selectGraphNode(node);
        }
        graph.cameraPosition(
          { x: node.x * 1.4, y: node.y * 1.4, z: node.z * 1.4 },
          node,
          800
        );
      })
      .onNodeHover((node) => {
        if (typeof global.updateGraphHoverDetails === "function") {
          global.updateGraphHoverDetails(node, null);
        }
      })
      .onLinkHover((link) => {
        if (typeof global.updateGraphHoverDetails === "function") {
          global.updateGraphHoverDetails(null, link);
        }
      })
      .onEngineStop(() => {
        // Keep a gentle orbit once layout settles
        if (autoRotate) startAutoRotate();
      });

    // Pause orbit while the user is interacting
    const controls = graph.controls();
    if (controls) {
      controls.addEventListener("start", () => {
        autoRotate = false;
        stopAutoRotate();
      });
    }

    window.addEventListener("resize", () => {
      if (!graph || !containerEl) return;
      graph.width(containerEl.clientWidth);
      graph.height(containerEl.clientHeight);
    });

    return graph;
  }

  function render(data) {
    const g = ensureGraph();
    if (!g || !data) return;

    const nodes = (data.nodes || []).map((n) => ({ ...n, id: Number(n.id) }));
    const direct = normalizeLinks(data.links || []);
    const indirect = normalizeLinks(data.indirect_connections || []).map((l) => ({
      ...l,
      type: "indirect",
    }));
    const links = [...direct, ...indirect];

    lastGraphData = { nodes, links };
    rebuildIndexes(lastGraphData);
    global.__graph3dNodeCount = nodes.length;
    global.__graph3dLinkCount = links.length;

    g.width(containerEl.clientWidth);
    g.height(containerEl.clientHeight);
    g.graphData({ nodes, links });

    // Seed a short warm-up orbit
    autoRotate = true;
    startAutoRotate();
  }

  function updateData(data) {
    if (!graph) {
      render(data);
      return;
    }
    render(data);
  }

  function destroy() {
    stopAutoRotate();
    if (graph) {
      try {
        graph._destructor && graph._destructor();
      } catch (_) {
        /* ignore */
      }
      graph = null;
    }
    if (containerEl) {
      containerEl.innerHTML = "";
    }
  }

  function findLink(fromId, toId) {
    return linkIndex.get(linkKey(fromId, toId)) || null;
  }

  function emitPacket(fromId, toId) {
    if (!graph) return false;
    const link = findLink(fromId, toId);
    if (!link) return false;
    link.__flashUntil = Date.now() + 1800;
    try {
      graph.emitParticle(link);
    } catch (err) {
      console.debug("emitParticle failed", err);
    }
    // Refresh colors briefly
    graph
      .linkColor(linkColor)
      .nodeColor(nodeColor);
    return true;
  }

  function emitPath(pathNodes) {
    if (!Array.isArray(pathNodes) || pathNodes.length < 2) return 0;
    let emitted = 0;
    for (let i = 0; i < pathNodes.length - 1; i++) {
      const delay = i * 180;
      const a = pathNodes[i];
      const b = pathNodes[i + 1];
      setTimeout(() => {
        if (emitPacket(a, b)) emitted += 1;
        const node = nodeById.get(Number(a));
        if (node) node.__flashUntil = Date.now() + 1200;
        const nodeB = nodeById.get(Number(b));
        if (nodeB) nodeB.__flashUntil = Date.now() + 1200;
        if (graph) graph.nodeColor(nodeColor);
      }, delay);
    }
    return pathNodes.length - 1;
  }

  function flashNodes(nodeIds) {
    if (!graph || !nodeIds) return;
    nodeIds.forEach((id) => {
      const node = nodeById.get(Number(id));
      if (node) node.__flashUntil = Date.now() + 1000;
    });
    graph.nodeColor(nodeColor);
  }

  function setAutoRotate(enabled) {
    autoRotate = !!enabled;
    if (autoRotate) startAutoRotate();
    else stopAutoRotate();
  }

  function center() {
    if (!graph) return;
    graph.zoomToFit(600, 40);
  }

  function isActive() {
    return !!graph;
  }

  global.NetworkGraph3D = {
    render,
    updateData,
    destroy,
    emitPacket,
    emitPath,
    flashNodes,
    setAutoRotate,
    center,
    isActive,
    getGraph: () => graph,
  };
})(window);
