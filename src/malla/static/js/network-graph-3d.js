/**
 * Orbital 3D Network Graph for Malla.
 *
 * Nodes are arranged on role-based orbital rings with always-visible
 * name + role labels. Camera uses OrbitControls (drag to orbit).
 */
(function (global) {
  "use strict";

  let graph = null;
  let containerEl = null;
  let autoRotate = true;
  let showLabels = true;
  let rotateHandle = null;
  let linkIndex = new Map();
  let nodeById = new Map();
  let lastGraphData = null;
  let labelObjects = new Map(); // nodeId -> sprite

  const ROLE_COLORS = {
    ROUTER: "#f59e0b",
    ROUTER_CLIENT: "#fb923c",
    REPEATER: "#ef4444",
    CLIENT: "#38bdf8",
    CLIENT_MUTE: "#818cf8",
    SENSOR: "#34d399",
    TRACKER: "#a78bfa",
    TAK: "#f472b6",
    TAK_TRACKER: "#e879f9",
    UNKNOWN: "#94a3b8",
  };

  function getTHREE() {
    return global.THREE || null;
  }

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

  function normalizeRole(role) {
    const r = String(role || "UNKNOWN")
      .trim()
      .toUpperCase()
      .replace(/\s+/g, "_");
    if (!r) return "UNKNOWN";
    if (r in ROLE_COLORS) return r;
    if (r.includes("ROUTER") && r.includes("CLIENT")) return "ROUTER_CLIENT";
    if (r.includes("ROUTER")) return "ROUTER";
    if (r.includes("REPEATER")) return "REPEATER";
    if (r.includes("CLIENT") && r.includes("MUTE")) return "CLIENT_MUTE";
    if (r.includes("CLIENT")) return "CLIENT";
    if (r.includes("SENSOR")) return "SENSOR";
    if (r.includes("TRACKER")) return "TRACKER";
    if (r.includes("TAK")) return "TAK";
    return "UNKNOWN";
  }

  function roleAbbrev(role) {
    const map = {
      ROUTER: "RTR",
      ROUTER_CLIENT: "RTR-C",
      REPEATER: "RPT",
      CLIENT: "CLI",
      CLIENT_MUTE: "MUTE",
      SENSOR: "SNS",
      TRACKER: "TRK",
      TAK: "TAK",
      TAK_TRACKER: "TAK-T",
      UNKNOWN: "?",
    };
    return map[normalizeRole(role)] || "?";
  }

  function displayLabel(node) {
    const shortName = (node.short_name || "").trim();
    const longName = (node.long_name || node.name || "").trim();
    let name = shortName || longName || `!${Number(node.id).toString(16)}`;
    if (name.length > 16) name = name.slice(0, 15) + "…";
    return `${name}\n${roleAbbrev(node.role)}`;
  }

  function nodeColor(node) {
    if (node.__flashUntil && Date.now() < node.__flashUntil) {
      return "#ffc107";
    }
    return ROLE_COLORS[normalizeRole(node.role)] || ROLE_COLORS.UNKNOWN;
  }

  function linkColor(link) {
    if (link.__flashUntil && Date.now() < link.__flashUntil) {
      return "#ffc107";
    }
    if (link.type === "indirect") return "rgba(255,152,0,0.28)";
    const snr = link.avg_snr;
    if (snr == null) return "rgba(148,163,184,0.4)";
    if (snr >= 0) return "rgba(52,211,153,0.7)";
    if (snr >= -10) return "rgba(56,189,248,0.65)";
    return "rgba(248,113,113,0.6)";
  }

  function orbitTier(nodes) {
    const tiers = [[], [], [], []];
    nodes.forEach((n) => {
      const role = normalizeRole(n.role);
      const connections = Number(n.connections) || 0;
      if (role === "ROUTER" || role === "REPEATER") {
        tiers[0].push(n);
      } else if (role === "ROUTER_CLIENT" || connections >= 6) {
        tiers[1].push(n);
      } else if (
        role === "CLIENT" ||
        role === "CLIENT_MUTE" ||
        connections >= 2
      ) {
        tiers[2].push(n);
      } else {
        tiers[3].push(n);
      }
    });
    // Avoid empty inner rings looking sparse: promote if needed
    if (tiers[0].length === 0 && tiers[1].length) {
      tiers[0] = tiers[1].splice(0, Math.max(1, Math.ceil(tiers[1].length / 4)));
    }
    return tiers;
  }

  function applyOrbitalLayout(nodes) {
    const tiers = orbitTier(nodes);
    const baseRadii = [36, 78, 126, 178];
    const count = Math.max(nodes.length, 1);
    // Scale orbits slightly with mesh size
    const scale = Math.min(2.2, Math.max(0.85, Math.sqrt(count) / 5));

    tiers.forEach((tierNodes, tierIdx) => {
      const n = tierNodes.length;
      if (!n) return;
      const radius = baseRadii[tierIdx] * scale;
      // Sort for stable positions across refreshes
      tierNodes.sort((a, b) => {
        const an = (a.short_name || a.name || "").toLowerCase();
        const bn = (b.short_name || b.name || "").toLowerCase();
        return an.localeCompare(bn) || Number(a.id) - Number(b.id);
      });
      tierNodes.forEach((node, i) => {
        const angle = (2 * Math.PI * i) / n + tierIdx * 0.35;
        const elev =
          Math.sin(angle * 2 + tierIdx) * (10 + tierIdx * 4) * scale * 0.35;
        node.x = radius * Math.cos(angle);
        node.y = elev;
        node.z = radius * Math.sin(angle);
        node.fx = node.x;
        node.fy = node.y;
        node.fz = node.z;
        node.__orbit = tierIdx;
      });
    });
  }

  function makeLabelSprite(text, color) {
    const THREE = getTHREE();
    if (!THREE) return null;

    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");
    const lines = String(text).split("\n");
    const fontMain = "600 28px Inter, Segoe UI, sans-serif";
    const fontSub = "600 20px Inter, Segoe UI, sans-serif";
    ctx.font = fontMain;
    let maxW = 0;
    lines.forEach((line, idx) => {
      ctx.font = idx === 0 ? fontMain : fontSub;
      maxW = Math.max(maxW, ctx.measureText(line).width);
    });
    const padX = 18;
    const padY = 12;
    const lineH = 30;
    canvas.width = Math.ceil(maxW + padX * 2);
    canvas.height = Math.ceil(lines.length * lineH + padY * 2);

    // Pill background
    const r = 14;
    ctx.fillStyle = "rgba(15, 23, 42, 0.82)";
    ctx.strokeStyle = color || "#94a3b8";
    ctx.lineWidth = 3;
    roundRect(ctx, 2, 2, canvas.width - 4, canvas.height - 4, r);
    ctx.fill();
    ctx.stroke();

    lines.forEach((line, idx) => {
      ctx.font = idx === 0 ? fontMain : fontSub;
      ctx.fillStyle = idx === 0 ? "#f8fafc" : color || "#cbd5e1";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(
        line,
        canvas.width / 2,
        padY + lineH * idx + lineH / 2
      );
    });

    const texture = new THREE.CanvasTexture(canvas);
    texture.minFilter = THREE.LinearFilter;
    texture.needsUpdate = true;
    const material = new THREE.SpriteMaterial({
      map: texture,
      transparent: true,
      depthTest: false,
      depthWrite: false,
    });
    const sprite = new THREE.Sprite(material);
    const aspect = canvas.width / canvas.height;
    const height = 9.5;
    sprite.scale.set(height * aspect, height, 1);
    sprite.center.set(0.5, 0);
    return sprite;
  }

  function roundRect(ctx, x, y, w, h, r) {
    const radius = Math.min(r, w / 2, h / 2);
    ctx.beginPath();
    ctx.moveTo(x + radius, y);
    ctx.arcTo(x + w, y, x + w, y + h, radius);
    ctx.arcTo(x + w, y + h, x, y + h, radius);
    ctx.arcTo(x, y + h, x, y, radius);
    ctx.arcTo(x, y, x + w, y, radius);
    ctx.closePath();
  }

  function buildNodeObject(node) {
    const THREE = getTHREE();
    if (!THREE) return undefined;

    const group = new THREE.Group();
    const color = nodeColor(node);
    const radius = Math.max(1.6, Math.min(5.5, (Number(node.size) || 5) * 0.45));

    const sphere = new THREE.Mesh(
      new THREE.SphereGeometry(radius, 18, 18),
      new THREE.MeshLambertMaterial({
        color,
        transparent: true,
        opacity: 0.95,
      })
    );
    group.add(sphere);

    // Soft halo for hubs / routers
    const role = normalizeRole(node.role);
    if (role === "ROUTER" || role === "REPEATER" || role === "ROUTER_CLIENT") {
      const halo = new THREE.Mesh(
        new THREE.SphereGeometry(radius * 1.55, 16, 16),
        new THREE.MeshBasicMaterial({
          color,
          transparent: true,
          opacity: 0.18,
          depthWrite: false,
        })
      );
      group.add(halo);
    }

    if (showLabels) {
      const sprite = makeLabelSprite(displayLabel(node), color);
      if (sprite) {
        sprite.position.y = radius + 2.2;
        group.add(sprite);
        labelObjects.set(Number(node.id), sprite);
      }
    }

    group.__nodeId = Number(node.id);
    return group;
  }

  function stopAutoRotate() {
    if (rotateHandle) {
      cancelAnimationFrame(rotateHandle);
      rotateHandle = null;
    }
    if (graph) {
      const controls = graph.controls();
      if (controls) controls.autoRotate = false;
    }
  }

  function startAutoRotate() {
    stopAutoRotate();
    if (!graph || !autoRotate) return;
    const controls = graph.controls();
    if (!controls) return;
    controls.autoRotate = true;
    controls.autoRotateSpeed = 0.55;
    const tick = () => {
      if (!graph || !autoRotate) return;
      if (typeof controls.update === "function") controls.update();
      rotateHandle = requestAnimationFrame(tick);
    };
    rotateHandle = requestAnimationFrame(tick);
  }

  function configureOrbitalCamera(g, nodes) {
    const controls = g.controls();
    if (controls) {
      controls.enableDamping = true;
      controls.dampingFactor = 0.08;
      controls.rotateSpeed = 0.7;
      controls.minDistance = 40;
      controls.maxDistance = 900;
      controls.enablePan = true;
    }

    // Frame the orbital rings
    const count = Math.max((nodes || []).length, 1);
    const scale = Math.min(2.2, Math.max(0.85, Math.sqrt(count) / 5));
    const dist = 220 * scale;
    g.cameraPosition({ x: dist * 0.75, y: dist * 0.45, z: dist * 0.85 }, { x: 0, y: 0, z: 0 }, 0);
  }

  function ensureGraph() {
    if (graph) return graph;
    if (typeof ForceGraph3D !== "function") {
      console.error("ForceGraph3D is not loaded");
      return null;
    }
    containerEl = document.getElementById("networkGraph");
    if (!containerEl) return null;

    const THREE = getTHREE();
    containerEl.innerHTML = "";

    const bg =
      getComputedStyle(document.body).getPropertyValue("--bs-body-bg") ||
      "#0b1220";

    graph = ForceGraph3D()(containerEl)
      .backgroundColor(bg.trim() || "#0b1220")
      .showNavInfo(false)
      .nodeId("id")
      .nodeLabel((n) => {
        const role = normalizeRole(n.role);
        const heard = n.last_seen
          ? new Date(n.last_seen * 1000).toLocaleString()
          : "unknown";
        const longName = n.long_name || n.name || n.id;
        const shortName = n.short_name ? ` (${n.short_name})` : "";
        return `${longName}${shortName}<br/>Role: ${role}<br/>Links: ${n.connections || 0}<br/>Last: ${heard}`;
      })
      .nodeThreeObject((node) => buildNodeObject(node))
      .nodeThreeObjectExtend(false)
      .nodeVal((n) => Math.max(2, Number(n.size) || 4))
      .nodeColor(nodeColor)
      .linkSource("source")
      .linkTarget("target")
      .linkColor(linkColor)
      .linkWidth((l) =>
        l.__flashUntil && Date.now() < l.__flashUntil
          ? 2.6
          : Math.max(0.45, Math.min(2.2, (l.strength || 1) * 0.7))
      )
      .linkOpacity(0.75)
      .linkDirectionalParticles(0)
      .linkDirectionalParticleWidth(3.2)
      .linkDirectionalParticleSpeed(0.009)
      .linkDirectionalParticleColor(() => "#fde68a")
      .warmupTicks(0)
      .cooldownTicks(0)
      .enableNodeDrag(false)
      .onNodeClick((node) => {
        if (typeof global.selectGraphNode === "function") {
          global.selectGraphNode(node);
        }
        const dist = 70;
        graph.cameraPosition(
          {
            x: node.x + dist * 0.6,
            y: node.y + dist * 0.4,
            z: node.z + dist * 0.7,
          },
          node,
          700
        );
      })
      .onNodeHover((node) => {
        if (typeof global.updateGraphHoverDetails === "function") {
          global.updateGraphHoverDetails(node, null);
        }
        if (containerEl) {
          containerEl.style.cursor = node ? "pointer" : "grab";
        }
      })
      .onLinkHover((link) => {
        if (typeof global.updateGraphHoverDetails === "function") {
          global.updateGraphHoverDetails(null, link);
        }
      });

    // Soften link forces; positions are pinned on orbital rings
    try {
      const linkForce = graph.d3Force("link");
      if (linkForce) linkForce.distance(28).strength(0.02);
      graph.d3Force("charge", null);
      graph.d3Force("center", null);
    } catch (_) {
      /* ignore */
    }

    if (THREE && graph.scene()) {
      // Subtle ambient + directional light for sphere shading
      const ambient = new THREE.AmbientLight(0xffffff, 0.75);
      const dir = new THREE.DirectionalLight(0xffffff, 0.55);
      dir.position.set(80, 120, 60);
      graph.scene().add(ambient);
      graph.scene().add(dir);
    }

    const controls = graph.controls();
    if (controls) {
      controls.addEventListener("start", () => {
        // Pause auto-orbit while user is interacting, resume after
        stopAutoRotate();
      });
      controls.addEventListener("end", () => {
        if (autoRotate) startAutoRotate();
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

    labelObjects = new Map();
    const nodes = (data.nodes || []).map((n) => ({ ...n, id: Number(n.id) }));
    applyOrbitalLayout(nodes);

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
    configureOrbitalCamera(g, nodes);

    autoRotate = document.getElementById("graphAutoRotate")
      ? document.getElementById("graphAutoRotate").checked
      : autoRotate;
    startAutoRotate();
  }

  function updateData(data) {
    render(data);
  }

  function destroy() {
    stopAutoRotate();
    labelObjects = new Map();
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
    graph.linkColor(linkColor);
    return true;
  }

  function emitPath(pathNodes) {
    if (!Array.isArray(pathNodes) || pathNodes.length < 2) return 0;
    for (let i = 0; i < pathNodes.length - 1; i++) {
      const delay = i * 180;
      const a = pathNodes[i];
      const b = pathNodes[i + 1];
      setTimeout(() => {
        emitPacket(a, b);
        const node = nodeById.get(Number(a));
        if (node) node.__flashUntil = Date.now() + 1200;
        const nodeB = nodeById.get(Number(b));
        if (nodeB) nodeB.__flashUntil = Date.now() + 1200;
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
    // Rebuild objects so sphere color updates for flash
    graph.nodeThreeObject((node) => buildNodeObject(node));
  }

  function setAutoRotate(enabled) {
    autoRotate = !!enabled;
    if (autoRotate) startAutoRotate();
    else stopAutoRotate();
  }

  function setShowLabels(enabled) {
    showLabels = !!enabled;
    if (lastGraphData) render(lastGraphData);
  }

  function center() {
    if (!graph || !lastGraphData) return;
    configureOrbitalCamera(graph, lastGraphData.nodes);
    graph.zoomToFit(700, 60);
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
    setShowLabels,
    center,
    isActive,
    getGraph: () => graph,
    ROLE_COLORS,
  };
})(window);
