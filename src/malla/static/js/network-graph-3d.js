/**
 * Mesh-style 3D Network Graph for Malla.
 *
 * Inspired by Remote-Terminal-for-MeshCore's PacketVisualizer3D:
 * force-directed layout, continuous drift, and colored particles
 * traveling along links for live packet traffic.
 */
(function (global) {
  "use strict";

  let graph = null;
  let containerEl = null;
  let autoRotate = false;
  let showLabels = true;
  let letEmDrift = true;
  let chargeStrength = 200;
  let particleSpeedMultiplier = 2;
  let rotateHandle = null;
  let linkIndex = new Map();
  let nodeById = new Map();
  let lastGraphData = null;
  let labelObjects = new Map();
  let stretchTimer = null;
  let baseCharge = 200;
  let nodeMeshes = new Map(); // nodeId -> { sphere, halo }
  let flashPaintHandle = null;
  let hasLayout = false;
  let driftTimer = null;
  const DRIFT_REHEAT_MS = 4000;

  // Meshtastic role colors (kept for role recognition)
  const ROLE_COLORS = {
    ROUTER: "#3b82f6",
    ROUTER_CLIENT: "#60a5fa",
    REPEATER: "#2563eb",
    CLIENT: "#f8fafc",
    CLIENT_MUTE: "#cbd5e1",
    SENSOR: "#34d399",
    TRACKER: "#a78bfa",
    TAK: "#f472b6",
    TAK_TRACKER: "#e879f9",
    UNKNOWN: "#9ca3af",
  };

  // Packet-type particle colors (MeshCore-inspired palette, Meshtastic portnums)
  const PACKET_COLORS = {
    NODEINFO_APP: "#f59e0b",
    POSITION_APP: "#f59e0b",
    TEXT_MESSAGE_APP: "#06b6d4",
    TEXT_MESSAGE_COMPRESSED_APP: "#06b6d4",
    ROUTING_APP: "#22c55e",
    TRACEROUTE_APP: "#f97316",
    TELEMETRY_APP: "#14b8a6",
    NEIGHBORINFO_APP: "#8b5cf6",
    STORE_FORWARD_APP: "#8b5cf6",
    ADMIN_APP: "#ec4899",
    RANGE_TEST_APP: "#ec4899",
    DETECTION_SENSOR_APP: "#34d399",
    PAXCOUNTER_APP: "#a78bfa",
    MAP_REPORT_APP: "#f59e0b",
    REMOTE_HARDWARE_APP: "#ec4899",
    UNKNOWN: "#6b7280",
  };

  const PACKET_LEGEND = [
    { key: "NODEINFO_APP", label: "INFO", color: PACKET_COLORS.NODEINFO_APP, description: "Node info / position" },
    { key: "TEXT_MESSAGE_APP", label: "TXT", color: PACKET_COLORS.TEXT_MESSAGE_APP, description: "Text message" },
    { key: "ROUTING_APP", label: "ACK", color: PACKET_COLORS.ROUTING_APP, description: "Routing / ACK" },
    { key: "TRACEROUTE_APP", label: "TR", color: PACKET_COLORS.TRACEROUTE_APP, description: "Traceroute" },
    { key: "TELEMETRY_APP", label: "TEL", color: PACKET_COLORS.TELEMETRY_APP, description: "Telemetry" },
    { key: "NEIGHBORINFO_APP", label: "NBR", color: PACKET_COLORS.NEIGHBORINFO_APP, description: "Neighbor / store-forward" },
    { key: "ADMIN_APP", label: "ADM", color: PACKET_COLORS.ADMIN_APP, description: "Admin / request" },
    { key: "UNKNOWN", label: "?", color: PACKET_COLORS.UNKNOWN, description: "Other" },
  ];

  const NODE_LEGEND = [
    { color: "#3b82f6", label: "Router / Repeater", size: 12 },
    { color: "#f8fafc", label: "Client", size: 10 },
    { color: "#34d399", label: "Sensor", size: 10 },
    { color: "#9ca3af", label: "Unknown", size: 10 },
  ];

  const BASE_PARTICLE_SPEED = 0.006;
  const HOP_DELAY_MS = 160;
  const LINK_DISTANCE = 120;

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

  function isInfrastructure(role) {
    const r = normalizeRole(role);
    return r === "ROUTER" || r === "REPEATER" || r === "ROUTER_CLIENT";
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

  function packetColor(portnumName) {
    const key = String(portnumName || "UNKNOWN").toUpperCase();
    return PACKET_COLORS[key] || PACKET_COLORS.UNKNOWN;
  }

  function nodeColor(node) {
    if (node.__flashUntil && Date.now() < node.__flashUntil) {
      return "#ffd700";
    }
    if (node.__pinned) return "#ffd700";
    return ROLE_COLORS[normalizeRole(node.role)] || ROLE_COLORS.UNKNOWN;
  }

  function linkColor(link) {
    if (link.__flashUntil && Date.now() < link.__flashUntil) {
      return link.__particleColor || "#fde68a";
    }
    if (link.type === "indirect") return "rgba(255,152,0,0.22)";
    const snr = link.avg_snr;
    if (snr == null) return "rgba(75,85,99,0.55)";
    if (snr >= 0) return "rgba(52,211,153,0.55)";
    if (snr >= -10) return "rgba(96,165,250,0.5)";
    return "rgba(248,113,113,0.45)";
  }

  function randomSpherePosition(radiusMin, radiusMax) {
    const theta = Math.random() * Math.PI * 2;
    const phi = Math.acos(2 * Math.random() - 1);
    const r = radiusMin + Math.random() * (radiusMax - radiusMin);
    return {
      x: r * Math.sin(phi) * Math.cos(theta),
      y: r * Math.sin(phi) * Math.sin(theta),
      z: r * Math.cos(phi),
    };
  }

  function seedForcePositions(nodes) {
    // Place hubs nearer the center, clients farther out — then let forces settle.
    nodes.forEach((node) => {
      const hub = isInfrastructure(node.role) || (Number(node.connections) || 0) >= 6;
      const pos = randomSpherePosition(hub ? 40 : 90, hub ? 110 : 220);
      node.x = pos.x;
      node.y = pos.y;
      node.z = pos.z;
      node.vx = 0;
      node.vy = 0;
      node.vz = 0;
      // Free positions — MeshCore-style force layout
      delete node.fx;
      delete node.fy;
      delete node.fz;
    });
  }

  function makeLabelSprite(text, color) {
    const THREE = getTHREE();
    if (!THREE) return null;

    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");
    const lines = String(text).split("\n");
    const fontMain = "600 26px 'Segoe UI', system-ui, sans-serif";
    const fontSub = "600 18px 'Segoe UI', system-ui, sans-serif";
    ctx.font = fontMain;
    let maxW = 0;
    lines.forEach((line, idx) => {
      ctx.font = idx === 0 ? fontMain : fontSub;
      maxW = Math.max(maxW, ctx.measureText(line).width);
    });
    const padX = 16;
    const padY = 10;
    const lineH = 28;
    canvas.width = Math.ceil(maxW + padX * 2);
    canvas.height = Math.ceil(lines.length * lineH + padY * 2);

    const r = 12;
    ctx.fillStyle = "rgba(10, 10, 14, 0.78)";
    ctx.strokeStyle = color || "#94a3b8";
    ctx.lineWidth = 2;
    roundRect(ctx, 2, 2, canvas.width - 4, canvas.height - 4, r);
    ctx.fill();
    ctx.stroke();

    lines.forEach((line, idx) => {
      ctx.font = idx === 0 ? fontMain : fontSub;
      ctx.fillStyle = idx === 0 ? "#f8fafc" : color || "#cbd5e1";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(line, canvas.width / 2, padY + lineH * idx + lineH / 2);
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
    const height = 8.5;
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
    const hub = isInfrastructure(node.role);
    const radius = Math.max(
      hub ? 2.8 : 1.8,
      Math.min(hub ? 6.5 : 4.5, (Number(node.size) || 5) * (hub ? 0.55 : 0.4))
    );

    const sphere = new THREE.Mesh(
      new THREE.SphereGeometry(radius, 20, 20),
      new THREE.MeshLambertMaterial({
        color,
        transparent: true,
        opacity: normalizeRole(node.role) === "CLIENT" ? 0.92 : 0.96,
      })
    );
    group.add(sphere);

    let halo = null;
    if (hub) {
      halo = new THREE.Mesh(
        new THREE.SphereGeometry(radius * 1.55, 16, 16),
        new THREE.MeshBasicMaterial({
          color,
          transparent: true,
          opacity: 0.16,
          depthWrite: false,
        })
      );
      group.add(halo);
    }

    if (showLabels) {
      const sprite = makeLabelSprite(displayLabel(node), color);
      if (sprite) {
        sprite.position.y = radius + 2.0;
        group.add(sprite);
        labelObjects.set(Number(node.id), sprite);
      }
    }

    group.__nodeId = Number(node.id);
    nodeMeshes.set(Number(node.id), { sphere, halo });
    return group;
  }

  function paintNodeColors() {
    const THREE = getTHREE();
    if (!THREE) return;
    nodeMeshes.forEach((meshes, id) => {
      const node = nodeById.get(Number(id));
      if (!node || !meshes || !meshes.sphere) return;
      const color = nodeColor(node);
      if (meshes.sphere.material && meshes.sphere.material.color) {
        meshes.sphere.material.color.set(color);
      }
      if (meshes.halo && meshes.halo.material && meshes.halo.material.color) {
        meshes.halo.material.color.set(color);
      }
    });
  }

  function scheduleFlashPaint() {
    if (flashPaintHandle) return;
    flashPaintHandle = requestAnimationFrame(() => {
      flashPaintHandle = null;
      paintNodeColors();
      // Keep refreshing while any node/link flash windows remain open
      const now = Date.now();
      let pending = false;
      nodeById.forEach((n) => {
        if (n.__flashUntil && n.__flashUntil > now) pending = true;
      });
      if (pending) scheduleFlashPaint();
    });
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
    controls.autoRotateSpeed = 0.4;
    const tick = () => {
      if (!graph || !autoRotate) return;
      if (typeof controls.update === "function") controls.update();
      rotateHandle = requestAnimationFrame(tick);
    };
    rotateHandle = requestAnimationFrame(tick);
  }

  function safeReheat() {
    if (!graph || !hasLayout) return;
    try {
      if (typeof graph.d3ReheatSimulation === "function") {
        graph.d3ReheatSimulation();
      }
    } catch (err) {
      console.debug("safeReheat:", err);
    }
  }

  function stopDriftTimer() {
    if (driftTimer) {
      clearInterval(driftTimer);
      driftTimer = null;
    }
  }

  function syncDriftTimer() {
    stopDriftTimer();
    if (!graph || !letEmDrift || !hasLayout) return;
    // Periodic reheat keeps the mesh alive without relying on the outer
    // alpha-target API (not exposed by ForceGraph3D) or unbounded cooldown,
    // which can race the first graphData digest and crash on layout.tick().
    driftTimer = setInterval(safeReheat, DRIFT_REHEAT_MS);
  }

  function applyDriftMode(reheat) {
    if (!graph) return;
    // Use long but finite cooldowns; continuous drift is done via syncDriftTimer.
    if (typeof graph.cooldownTicks === "function") {
      graph.cooldownTicks(letEmDrift ? 300 : 180);
    }
    if (typeof graph.cooldownTime === "function") {
      graph.cooldownTime(letEmDrift ? 20000 : 15000);
    }
    if (reheat) safeReheat();
    syncDriftTimer();
  }

  function applyForceSettings(options) {
    if (!graph) return;
    const opts = options || {};
    try {
      const linkForce = graph.d3Force("link");
      if (linkForce) {
        linkForce.distance(LINK_DISTANCE).strength(0.35);
      }

      const charge = graph.d3Force("charge");
      if (charge && typeof charge.strength === "function") {
        charge.strength(-Math.abs(chargeStrength));
        if (typeof charge.distanceMax === "function") {
          charge.distanceMax(800);
        }
      }

      const center = graph.d3Force("center");
      if (center && typeof center.strength === "function") {
        center.strength(0.05);
      }

      if (typeof graph.d3AlphaDecay === "function") {
        graph.d3AlphaDecay(0.0228);
      }
      if (typeof graph.d3VelocityDecay === "function") {
        graph.d3VelocityDecay(0.4);
      }
      applyDriftMode(!!opts.reheat);
    } catch (err) {
      console.debug("applyForceSettings:", err);
    }
  }

  function configureCamera(g, nodes) {
    const controls = g.controls();
    if (controls) {
      controls.enableDamping = true;
      controls.dampingFactor = 0.08;
      controls.rotateSpeed = 0.7;
      controls.minDistance = 30;
      controls.maxDistance = 1200;
      controls.enablePan = true;
    }

    const count = Math.max((nodes || []).length, 1);
    const dist = Math.min(520, Math.max(180, 90 + Math.sqrt(count) * 38));
    g.cameraPosition(
      { x: dist * 0.7, y: dist * 0.4, z: dist * 0.85 },
      { x: 0, y: 0, z: 0 },
      0
    );
  }

  function particleSpeed() {
    return BASE_PARTICLE_SPEED * Math.max(0.5, Math.min(5, particleSpeedMultiplier));
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

    graph = ForceGraph3D()(containerEl)
      .backgroundColor("#0a0a0a")
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
          ? 2.4
          : Math.max(0.4, Math.min(2.0, (l.strength || 1) * 0.65))
      )
      .linkOpacity(0.7)
      .linkDirectionalParticles(0)
      .linkDirectionalParticleWidth(3.4)
      .linkDirectionalParticleSpeed(() => particleSpeed())
      .linkDirectionalParticleColor((l) => l.__particleColor || "#fde68a")
      .warmupTicks(0)
      .cooldownTicks(300)
      .cooldownTime(20000)
      .enableNodeDrag(true)
      .onNodeClick((node) => {
        if (typeof global.selectGraphNode === "function") {
          global.selectGraphNode(node);
        }
        const dist = 80;
        graph.cameraPosition(
          {
            x: node.x + dist * 0.55,
            y: node.y + dist * 0.35,
            z: node.z + dist * 0.65,
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

    // Force layout tuning (no reheat here — graphData hasn't run yet)
    try {
      const linkForce = graph.d3Force("link");
      if (linkForce) linkForce.distance(LINK_DISTANCE).strength(0.35);

      const charge = graph.d3Force("charge");
      if (charge && typeof charge.strength === "function") {
        charge.strength(-Math.abs(chargeStrength));
        if (typeof charge.distanceMax === "function") {
          charge.distanceMax(800);
        }
      }

      const center = graph.d3Force("center");
      if (center && typeof center.strength === "function") {
        center.strength(0.05);
      }
    } catch (_) {
      /* ignore */
    }

    if (THREE && graph.scene()) {
      try {
        const ambient = new THREE.AmbientLight(0xffffff, 0.7);
        const dir = new THREE.DirectionalLight(0xffffff, 0.55);
        dir.position.set(80, 120, 60);
        graph.scene().add(ambient);
        graph.scene().add(dir);
      } catch (_) {
        /* ignore dual-three issues */
      }
    }

    const controls = graph.controls();
    if (controls) {
      controls.addEventListener("start", () => {
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
    nodeMeshes = new Map();
    const nodes = (data.nodes || []).map((n) => ({ ...n, id: Number(n.id) }));
    seedForcePositions(nodes);

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

    // Configure forces BEFORE graphData so the update uses them; do not reheat yet.
    applyForceSettings({ reheat: false });

    g.width(containerEl.clientWidth);
    g.height(containerEl.clientHeight);
    g.graphData({ nodes, links });
    // Mark layout ready after a turn so kapsule's graphData digest can finish.
    // Never reheat synchronously here — that races engineRunning vs state.layout.
    setTimeout(() => {
      if (!graph) return;
      hasLayout = true;
      applyDriftMode(false);
    }, 0);
    configureCamera(g, nodes);

    autoRotate = document.getElementById("graphAutoRotate")
      ? document.getElementById("graphAutoRotate").checked
      : autoRotate;
    if (autoRotate) startAutoRotate();
    else stopAutoRotate();

    // Soft zoom-to-fit after initial settle
    setTimeout(() => {
      if (graph) {
        try {
          graph.zoomToFit(600, 80);
        } catch (_) {
          /* ignore */
        }
      }
    }, 900);
  }

  function updateData(data) {
    if (!graph || !data) {
      render(data);
      return;
    }

    // Preserve positions of existing nodes across refresh
    const prevPos = new Map();
    (lastGraphData && lastGraphData.nodes ? lastGraphData.nodes : []).forEach((n) => {
      prevPos.set(Number(n.id), { x: n.x, y: n.y, z: n.z, vx: n.vx, vy: n.vy, vz: n.vz });
    });

    labelObjects = new Map();
    nodeMeshes = new Map();
    const nodes = (data.nodes || []).map((n) => {
      const id = Number(n.id);
      const prev = prevPos.get(id);
      const next = { ...n, id };
      if (prev && prev.x != null) {
        next.x = prev.x;
        next.y = prev.y;
        next.z = prev.z;
        next.vx = prev.vx || 0;
        next.vy = prev.vy || 0;
        next.vz = prev.vz || 0;
      } else {
        const pos = randomSpherePosition(100, 200);
        next.x = pos.x;
        next.y = pos.y;
        next.z = pos.z;
      }
      delete next.fx;
      delete next.fy;
      delete next.fz;
      return next;
    });

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

    applyForceSettings({ reheat: false });
    graph.graphData({ nodes, links });
    setTimeout(() => {
      if (!graph) return;
      hasLayout = true;
      applyDriftMode(false);
    }, 0);
  }

  function destroy() {
    stopAutoRotate();
    stopDriftTimer();
    if (stretchTimer) {
      clearTimeout(stretchTimer);
      stretchTimer = null;
    }
    if (flashPaintHandle) {
      cancelAnimationFrame(flashPaintHandle);
      flashPaintHandle = null;
    }
    labelObjects = new Map();
    nodeMeshes = new Map();
    hasLayout = false;
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

  function emitPacket(fromId, toId, options) {
    if (!graph) return false;
    const link = findLink(fromId, toId);
    if (!link) return false;
    const opts = options || {};
    const color = packetColor(opts.portnum);
    link.__particleColor = color;
    link.__flashUntil = Date.now() + 1800;
    try {
      graph.emitParticle(link);
    } catch (err) {
      console.debug("emitParticle failed", err);
    }
    return true;
  }

  function emitPath(pathNodes, options) {
    if (!Array.isArray(pathNodes) || pathNodes.length < 2) return 0;
    const opts = options || {};
    for (let i = 0; i < pathNodes.length - 1; i++) {
      const delay = i * HOP_DELAY_MS;
      const a = pathNodes[i];
      const b = pathNodes[i + 1];
      setTimeout(() => {
        if (!graph) return;
        emitPacket(a, b, opts);
        const node = nodeById.get(Number(a));
        if (node) node.__flashUntil = Date.now() + 1200;
        const nodeB = nodeById.get(Number(b));
        if (nodeB) nodeB.__flashUntil = Date.now() + 1200;
        // In-place color update — never rebuild nodeThreeObject during the force tick
        scheduleFlashPaint();
      }, delay);
    }
    return pathNodes.length - 1;
  }

  function flashNodes(nodeIds) {
    if (!graph || !nodeIds) return;
    const ids = nodeIds instanceof Set ? Array.from(nodeIds) : nodeIds;
    ids.forEach((id) => {
      const node = nodeById.get(Number(id));
      if (node) node.__flashUntil = Date.now() + 1000;
    });
    scheduleFlashPaint();
  }

  function setAutoRotate(enabled) {
    autoRotate = !!enabled;
    if (autoRotate) startAutoRotate();
    else stopAutoRotate();
  }

  function setShowLabels(enabled) {
    const next = !!enabled;
    if (next === showLabels) return;
    showLabels = next;
    if (lastGraphData && graph) {
      // Full object rebuild only when the label toggle actually changes
      labelObjects = new Map();
      nodeMeshes = new Map();
      graph.nodeThreeObject((node) => buildNodeObject(node));
    }
  }

  function setLetEmDrift(enabled) {
    letEmDrift = !!enabled;
    if (!graph) return;
    applyDriftMode(hasLayout);
  }

  function setRepulsion(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return;
    const next = Math.max(50, Math.min(2500, n));
    const changed = next !== chargeStrength;
    chargeStrength = next;
    baseCharge = chargeStrength;
    // Only reheat when the user actually changes repulsion (not initial sync)
    applyForceSettings({ reheat: changed && hasLayout });
  }

  function setParticleSpeed(multiplier) {
    const n = Number(multiplier);
    if (!Number.isFinite(n)) return;
    particleSpeedMultiplier = Math.max(0.5, Math.min(5, n));
  }

  function shuffleLayout() {
    if (!graph || !lastGraphData) return;
    lastGraphData.nodes.forEach((node) => {
      const pos = randomSpherePosition(40, 220);
      node.x = pos.x;
      node.y = pos.y;
      node.z = pos.z;
      node.vx = (Math.random() - 0.5) * 4;
      node.vy = (Math.random() - 0.5) * 4;
      node.vz = (Math.random() - 0.5) * 4;
      delete node.fx;
      delete node.fy;
      delete node.fz;
    });
    graph.graphData({ nodes: lastGraphData.nodes, links: lastGraphData.links });
    hasLayout = true;
    applyDriftMode(true);
  }

  function expandContract() {
    if (!graph) return;
    if (stretchTimer) {
      clearTimeout(stretchTimer);
      stretchTimer = null;
    }
    // Temporarily crank repulsion ("Oooh Big Stretch!")
    const peak = Math.max(chargeStrength * 4, 800);
    chargeStrength = peak;
    applyForceSettings({ reheat: true });

    stretchTimer = setTimeout(() => {
      chargeStrength = baseCharge;
      applyForceSettings({ reheat: true });
      stretchTimer = null;
    }, 1400);
  }

  function center() {
    if (!graph || !lastGraphData) return;
    configureCamera(graph, lastGraphData.nodes);
    graph.zoomToFit(700, 70);
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
    setLetEmDrift,
    setRepulsion,
    setParticleSpeed,
    shuffleLayout,
    expandContract,
    center,
    isActive,
    getGraph: () => graph,
    packetColor,
    ROLE_COLORS,
    PACKET_COLORS,
    PACKET_LEGEND,
    NODE_LEGEND,
  };
})(window);
