'use strict';
const MANIFEST = 'flutter-app-manifest';
const TEMP = 'flutter-temp-cache';
const CACHE_NAME = 'flutter-app-cache';

const RESOURCES = {".git/COMMIT_EDITMSG": "6d03382fe6cac7fca71075ac6b34d183",
".git/config": "6c243f7c4037071d4573362169923c36",
".git/description": "a0a7c3fff21f2aea3cfa1d0316dd816c",
".git/HEAD": "cf7dd3ce51958c5f13fece957cc417fb",
".git/hooks/applypatch-msg.sample": "ce562e08d8098926a3862fc6e7905199",
".git/hooks/commit-msg.sample": "579a3c1e12a1e74a98169175fb913012",
".git/hooks/fsmonitor-watchman.sample": "a0b2633a2c8e97501610bd3f73da66fc",
".git/hooks/post-update.sample": "2b7ea5cee3c49ff53d41e00785eb974c",
".git/hooks/pre-applypatch.sample": "054f9ffb8bfe04a599751cc757226dda",
".git/hooks/pre-commit.sample": "5029bfab85b1c39281aa9697379ea444",
".git/hooks/pre-merge-commit.sample": "39cb268e2a85d436b9eb6f47614c3cbc",
".git/hooks/pre-push.sample": "2c642152299a94e05ea26eae11993b13",
".git/hooks/pre-rebase.sample": "56e45f2bcbc8226d2b4200f7c46371bf",
".git/hooks/pre-receive.sample": "2ad18ec82c20af7b5926ed9cea6aeedd",
".git/hooks/prepare-commit-msg.sample": "2b5c047bdb474555e1787db32b2d2fc5",
".git/hooks/push-to-checkout.sample": "c7ab00c7784efeadad3ae9b228d4b4db",
".git/hooks/sendemail-validate.sample": "4d67df3a8d5c98cb8565c07e42be0b04",
".git/hooks/update.sample": "647ae13c682f7827c22f5fc08a03674e",
".git/index": "e2d55a0011d9c78962ae2b5cbeaea17d",
".git/info/exclude": "036208b4a1ab4a235d75c181e685e5a3",
".git/logs/HEAD": "7beb442980d4b1eee9de6dc6c6e1d6c2",
".git/logs/refs/heads/main": "4af5dd09a818f2596b26bc39b1d6938c",
".git/logs/refs/remotes/origin/main": "ae9aa6b7f6b2cafa5babb305607dd19e",
".git/objects/02/1d4f3579879a4ac147edbbd8ac2d91e2bc7323": "9e9721befbee4797263ad5370cd904ff",
".git/objects/06/79c8388c341bd7e94effedb0a197d432cab245": "178f49c4208493bffe61a788e98c7086",
".git/objects/07/cea66e54218b005b2f07665867d2e98dedc3e8": "6ba207ee553b6c23535b3d437d51934e",
".git/objects/08/f8dd7e3ebf0ff335a820996766ae077d96f101": "516c281326e307be1b0c125b3d72fdc6",
".git/objects/09/b8cce2eb1892780cf8c1ee67435251a9b54012": "5bc21cd1e969734f5e25d6624d50528c",
".git/objects/0b/9fcf3d6c6058acc662279d9d22099086a0c78a": "0f20d8b31472ed851f3506e98bb44282",
".git/objects/11/149db9603ff85743af778014e6fe3687ece72a": "f4982b697661627e854119007882903d",
".git/objects/14/26fc190dc577c77dcf7d89c91718456bddcbe8": "34379380938c344b6cba47d1f498e37c",
".git/objects/1c/b02b3366610188f1cd1f94333754debce2dbf6": "c6f3936fe0ddcca24f730277445dec76",
".git/objects/20/3a3ff5cc524ede7e585dff54454bd63a1b0f36": "4b23a88a964550066839c18c1b5c461e",
".git/objects/29/f22f56f0c9903bf90b2a78ef505b36d89a9725": "e85914d97d264694217ae7558d414e81",
".git/objects/37/75d1e57d6432e7e9f9d44d6d215d6666adb684": "2611412c4a4e5decf5f94fb4b456cf72",
".git/objects/3a/7adddc7ae78836f3af193bd55de3adf0caf24f": "7b215c2603976968238c4a33c3dd6e34",
".git/objects/3d/f4814e0f954683d890a76c50152a4da07406dd": "e4d7d1e343fb45dd2b77667c62eed796",
".git/objects/46/4ab5882a2234c39b1a4dbad5feba0954478155": "2e52a767dc04391de7b4d0beb32e7fc4",
".git/objects/4c/1c9bc0def6dfeffce4d8adaaa44286796d2dad": "30609ab711c750070a33536aad445f77",
".git/objects/4d/bf9da7bcce5387354fe394985b98ebae39df43": "534c022f4a0845274cbd61ff6c9c9c33",
".git/objects/4f/fbe6ec4693664cb4ff395edf3d949bd4607391": "2beb9ca6c799e0ff64e0ad79f9e55e69",
".git/objects/50/24c69dd343c15b739220c46bfaf97f7df49ca5": "71da4a5f897d0ba3b8d5fae4803e87ff",
".git/objects/53/c39112ee3baa7718fe442dd1cb960bf8bd313f": "d2fdeba3a40498fd14ad98db517c6459",
".git/objects/59/6aefa08ad68be9c2c9b3047052c875ef5feb4f": "b748b8a99d241a4aaa62ee1c46ae3a60",
".git/objects/64/0f17ef9c62d3e42f4e25105ad31de1aca3425e": "ac3a18ec44bf8868ac6e8aa7e501b52e",
".git/objects/67/46c64ddb22ea3f9c2f0d7168d9ea15a3ea55ba": "da2e70fc9884e27c236fc716749beff0",
".git/objects/67/6bf016152d5a5047aaf4462ef9afb06830fda8": "73d626db1a8a15da2849a5e4175c3d0b",
".git/objects/6b/9862a1351012dc0f337c9ee5067ed3dbfbb439": "85896cd5fba127825eb58df13dfac82b",
".git/objects/7a/6c1911dddaea52e2dbffc15e45e428ec9a9915": "f1dee6885dc6f71f357a8e825bda0286",
".git/objects/83/7cc483b2a190f5be539c0bd6bbb37f16d4f1a9": "c913977bfe00e8f46ca038a11641f4df",
".git/objects/86/03d0a3d2a91580f77171968c7d13e73fd1482a": "dc750bd17c929d834d260dd7dc0293e7",
".git/objects/88/cfd48dff1169879ba46840804b412fe02fefd6": "e42aaae6a4cbfbc9f6326f1fa9e3380c",
".git/objects/8f/c05e285f3d3e0d203b883015acb40914cba85a": "c4bfe74a32f7bf85733ff43433f2bd2e",
".git/objects/91/687e71a3af6fd6898e7cbf0bdb325756f1e845": "2d758146e16a42c0af3b8abf834e8ada",
".git/objects/98/0d49437042d93ffa850a60d02cef584a35a85c": "8e18e4c1b6c83800103ff097cc222444",
".git/objects/9b/3ef5f169177a64f91eafe11e52b58c60db3df2": "91d370e4f73d42e0a622f3e44af9e7b1",
".git/objects/9e/3b4630b3b8461ff43c272714e00bb47942263e": "accf36d08c0545fa02199021e5902d52",
".git/objects/a4/09a7447c43d4ef5a81cae65d826177013a163a": "07b9579f45783959c4f1efc02da84e91",
".git/objects/ab/77a1512ea0496de000f8c21f64cddaed42154f": "744a48053ab898fbff6ef21034a32a2f",
".git/objects/b1/9661ae4839b6979a46925cfa549beec5c391ba": "e27d9c67e73a5ffba175396e52c91e3c",
".git/objects/b6/b8806f5f9d33389d53c2868e6ea1aca7445229": "b14016efdbcda10804235f3a45562bbf",
".git/objects/b7/49bfef07473333cf1dd31e9eed89862a5d52aa": "36b4020dca303986cad10924774fb5dc",
".git/objects/b9/2a0d854da9a8f73216c4a0ef07a0f0a44e4373": "f62d1eb7f51165e2a6d2ef1921f976f3",
".git/objects/c2/a92af91bddad1e5c9180512f67afb693c73f17": "81fc8d6757b7a85925eb30efb06c49c4",
".git/objects/c4/016f7d68c0d70816a0c784867168ffa8f419e1": "fdf8b8a8484741e7a3a558ed9d22f21d",
".git/objects/c8/9cc4f891ab159889038a0b4fa2764e6e285770": "aab10870b3faf47030f2ac2cd2f07a0e",
".git/objects/ca/3bba02c77c467ef18cffe2d4c857e003ad6d5d": "316e3d817e75cf7b1fd9b0226c088a43",
".git/objects/d2/ce4463f5b2620fd7f56340df8a777b8f59e242": "6d90ff9680895bab3a638eab5f43a2f7",
".git/objects/d3/e90342cbf33ecf06ab006fd4c8e50a9f7a6098": "ff899337399b774daa9fc9ff2483ecc7",
".git/objects/d4/3532a2348cc9c26053ddb5802f0e5d4b8abc05": "3dad9b209346b1723bb2cc68e7e42a44",
".git/objects/d6/59d9107130fa17c1cb475c128ab2e4e52a76e6": "666597d96b9a3179959f7b3458174d44",
".git/objects/d6/9c56691fbdb0b7efa65097c7cc1edac12a6d3e": "868ce37a3a78b0606713733248a2f579",
".git/objects/de/88cfc72fe4818c9374e0b54cf84d1caab810b3": "f78405a981500121d5305cfdbe339ee7",
".git/objects/e0/7797437d096064bd90c373800dcb0f335c14b0": "16f9b9defb16491f8c733b09b022688c",
".git/objects/e1/2569382f7dc8d1cb540bfe378b8a0d1ff614ee": "3cbdb419aa03b6761ef1c1c40be44bc4",
".git/objects/e3/e9ee754c75ae07cc3d19f9b8c1e656cc4946a1": "14066365125dcce5aec8eb1454f0d127",
".git/objects/e5/4a99ec367e67d61e6e62ea8edceb913a142a85": "6e5f38253bd3e47738e28d228ff1346d",
".git/objects/e6/8dc20e32ae31e94668357e654706602909edd7": "d4aef040039c4f81c3cce5ee3ccbc500",
".git/objects/e7/730105f7cb2baa2593a59212f25cd19831923e": "d1f1e75067e0af9114cd40a7d791941a",
".git/objects/e9/94225c71c957162e2dcc06abe8295e482f93a2": "2eed33506ed70a5848a0b06f5b754f2c",
".git/objects/eb/9b4d76e525556d5d89141648c724331630325d": "37c0954235cbe27c4d93e74fe9a578ef",
".git/objects/ed/b55d4deb8363b6afa65df71d1f9fd8c7787f22": "886ebb77561ff26a755e09883903891d",
".git/objects/f2/04823a42f2d890f945f70d88b8e2d921c6ae26": "6b47f314ffc35cf6a1ced3208ecc857d",
".git/objects/f2/d5db364eeb53c12863a20d2efb1931c4fd38ff": "6b5aac8004d158d4aa38c4568b23c279",
".git/objects/f5/72b90ef57ee79b82dd846c6871359a7cb10404": "e68f5265f0bb82d792ff536dcb99d803",
".git/objects/fe/3b987e61ed346808d9aa023ce3073530ad7426": "dc7db10bf25046b27091222383ede515",
".git/objects/fe/d0e7884e784b0eee96d46989a8e83bc56afc62": "7c74d6046f785d9497e237e9776cc703",
".git/objects/ff/20a20a60b43063d8751b5da57670210804453a": "1274c234e0e18c7c0725aeb0fbcd8a8a",
".git/refs/heads/main": "b1591c6bc7aa51b4b3904cfeff984993",
".git/refs/remotes/origin/main": "b1591c6bc7aa51b4b3904cfeff984993",
"assets/AssetManifest.bin": "330798dcc94b9f1de79dfe8c1fc59cd0",
"assets/AssetManifest.bin.json": "b9d47010bc36b83aff8bc78b5dab4d05",
"assets/AssetManifest.json": "d2f0490bdbfe923f36450ab9f5caf3b1",
"assets/assets/areas/1.png": "b0da28cf82001b9702b1d9d7143e1d12",
"assets/assets/areas/2.png": "5dddfa4593023d675ddd81d5b35479ea",
"assets/assets/images/google.png": "16557008de1d50aaad00bffc7c14852a",
"assets/assets/images/user.png": "29479ba0435741580ca9f4a467be6207",
"assets/FontManifest.json": "dc3d03800ccca4601324923c0b1d6d57",
"assets/fonts/MaterialIcons-Regular.otf": "6535852252d366240adbc4f90e22d042",
"assets/NOTICES": "a3e8f810b061ff855de096f5671ca4be",
"assets/packages/cupertino_icons/assets/CupertinoIcons.ttf": "33b7d9392238c04c131b6ce224e13711",
"assets/packages/flutter_map/lib/assets/flutter_map_logo.png": "208d63cc917af9713fc9572bd5c09362",
"assets/shaders/ink_sparkle.frag": "ecc85a2e95f5e9f53123dcaf8cb9b6ce",
"canvaskit/canvaskit.js": "140ccb7d34d0a55065fbd422b843add6",
"canvaskit/canvaskit.js.symbols": "58832fbed59e00d2190aa295c4d70360",
"canvaskit/canvaskit.wasm": "07b9f5853202304d3b0749d9306573cc",
"canvaskit/chromium/canvaskit.js": "5e27aae346eee469027c80af0751d53d",
"canvaskit/chromium/canvaskit.js.symbols": "193deaca1a1424049326d4a91ad1d88d",
"canvaskit/chromium/canvaskit.wasm": "24c77e750a7fa6d474198905249ff506",
"canvaskit/skwasm.js": "1ef3ea3a0fec4569e5d531da25f34095",
"canvaskit/skwasm.js.symbols": "0088242d10d7e7d6d2649d1fe1bda7c1",
"canvaskit/skwasm.wasm": "264db41426307cfc7fa44b95a7772109",
"canvaskit/skwasm_heavy.js": "413f5b2b2d9345f37de148e2544f584f",
"canvaskit/skwasm_heavy.js.symbols": "3c01ec03b5de6d62c34e17014d1decd3",
"canvaskit/skwasm_heavy.wasm": "8034ad26ba2485dab2fd49bdd786837b",
"favicon.png": "ee5d486f6d1b1b5bdc06c2846895af35",
"flutter.js": "888483df48293866f9f41d3d9274a779",
"flutter_bootstrap.js": "fd475246d2247b8f19ede49d36e28286",
"icons/Icon-192.png": "ac9a721a12bbc803b44f645561ecb1e1",
"icons/Icon-512.png": "96e752610906ba2a93c65f8abe1645f1",
"icons/Icon-maskable-192.png": "c457ef57daa1d16f64b27b786ec2ea3c",
"icons/Icon-maskable-512.png": "301a7604d45b3e739efc881eb04896ea",
"index.html": "fbedd2991500a13aaeef42d1ed20b56d",
"/": "fbedd2991500a13aaeef42d1ed20b56d",
"main.dart.js": "537d7b6819b71a4837858cf84a7ee971",
"manifest.json": "8045ebc5ee2b8a0aeab643d13a92512b",
"version.json": "f0be9e0ae60b13db8fe973642d3b7241"};
// The application shell files that are downloaded before a service worker can
// start.
const CORE = ["main.dart.js",
"index.html",
"flutter_bootstrap.js",
"assets/AssetManifest.bin.json",
"assets/FontManifest.json"];

// During install, the TEMP cache is populated with the application shell files.
self.addEventListener("install", (event) => {
  self.skipWaiting();
  return event.waitUntil(
    caches.open(TEMP).then((cache) => {
      return cache.addAll(
        CORE.map((value) => new Request(value, {'cache': 'reload'})));
    })
  );
});
// During activate, the cache is populated with the temp files downloaded in
// install. If this service worker is upgrading from one with a saved
// MANIFEST, then use this to retain unchanged resource files.
self.addEventListener("activate", function(event) {
  return event.waitUntil(async function() {
    try {
      var contentCache = await caches.open(CACHE_NAME);
      var tempCache = await caches.open(TEMP);
      var manifestCache = await caches.open(MANIFEST);
      var manifest = await manifestCache.match('manifest');
      // When there is no prior manifest, clear the entire cache.
      if (!manifest) {
        await caches.delete(CACHE_NAME);
        contentCache = await caches.open(CACHE_NAME);
        for (var request of await tempCache.keys()) {
          var response = await tempCache.match(request);
          await contentCache.put(request, response);
        }
        await caches.delete(TEMP);
        // Save the manifest to make future upgrades efficient.
        await manifestCache.put('manifest', new Response(JSON.stringify(RESOURCES)));
        // Claim client to enable caching on first launch
        self.clients.claim();
        return;
      }
      var oldManifest = await manifest.json();
      var origin = self.location.origin;
      for (var request of await contentCache.keys()) {
        var key = request.url.substring(origin.length + 1);
        if (key == "") {
          key = "/";
        }
        // If a resource from the old manifest is not in the new cache, or if
        // the MD5 sum has changed, delete it. Otherwise the resource is left
        // in the cache and can be reused by the new service worker.
        if (!RESOURCES[key] || RESOURCES[key] != oldManifest[key]) {
          await contentCache.delete(request);
        }
      }
      // Populate the cache with the app shell TEMP files, potentially overwriting
      // cache files preserved above.
      for (var request of await tempCache.keys()) {
        var response = await tempCache.match(request);
        await contentCache.put(request, response);
      }
      await caches.delete(TEMP);
      // Save the manifest to make future upgrades efficient.
      await manifestCache.put('manifest', new Response(JSON.stringify(RESOURCES)));
      // Claim client to enable caching on first launch
      self.clients.claim();
      return;
    } catch (err) {
      // On an unhandled exception the state of the cache cannot be guaranteed.
      console.error('Failed to upgrade service worker: ' + err);
      await caches.delete(CACHE_NAME);
      await caches.delete(TEMP);
      await caches.delete(MANIFEST);
    }
  }());
});
// The fetch handler redirects requests for RESOURCE files to the service
// worker cache.
self.addEventListener("fetch", (event) => {
  if (event.request.method !== 'GET') {
    return;
  }
  var origin = self.location.origin;
  var key = event.request.url.substring(origin.length + 1);
  // Redirect URLs to the index.html
  if (key.indexOf('?v=') != -1) {
    key = key.split('?v=')[0];
  }
  if (event.request.url == origin || event.request.url.startsWith(origin + '/#') || key == '') {
    key = '/';
  }
  // If the URL is not the RESOURCE list then return to signal that the
  // browser should take over.
  if (!RESOURCES[key]) {
    return;
  }
  // If the URL is the index.html, perform an online-first request.
  if (key == '/') {
    return onlineFirst(event);
  }
  event.respondWith(caches.open(CACHE_NAME)
    .then((cache) =>  {
      return cache.match(event.request).then((response) => {
        // Either respond with the cached resource, or perform a fetch and
        // lazily populate the cache only if the resource was successfully fetched.
        return response || fetch(event.request).then((response) => {
          if (response && Boolean(response.ok)) {
            cache.put(event.request, response.clone());
          }
          return response;
        });
      })
    })
  );
});
self.addEventListener('message', (event) => {
  // SkipWaiting can be used to immediately activate a waiting service worker.
  // This will also require a page refresh triggered by the main worker.
  if (event.data === 'skipWaiting') {
    self.skipWaiting();
    return;
  }
  if (event.data === 'downloadOffline') {
    downloadOffline();
    return;
  }
});
// Download offline will check the RESOURCES for all files not in the cache
// and populate them.
async function downloadOffline() {
  var resources = [];
  var contentCache = await caches.open(CACHE_NAME);
  var currentContent = {};
  for (var request of await contentCache.keys()) {
    var key = request.url.substring(origin.length + 1);
    if (key == "") {
      key = "/";
    }
    currentContent[key] = true;
  }
  for (var resourceKey of Object.keys(RESOURCES)) {
    if (!currentContent[resourceKey]) {
      resources.push(resourceKey);
    }
  }
  return contentCache.addAll(resources);
}
// Attempt to download the resource online before falling back to
// the offline cache.
function onlineFirst(event) {
  return event.respondWith(
    fetch(event.request).then((response) => {
      return caches.open(CACHE_NAME).then((cache) => {
        cache.put(event.request, response.clone());
        return response;
      });
    }).catch((error) => {
      return caches.open(CACHE_NAME).then((cache) => {
        return cache.match(event.request).then((response) => {
          if (response != null) {
            return response;
          }
          throw error;
        });
      });
    })
  );
}
