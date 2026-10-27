import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

import release_material as material

spec = importlib.util.spec_from_file_location("publisher", Path(__file__).with_name("publish-release.py"))
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)


class MaterialTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dist = self.root / "dist"
        self.dist.mkdir()
        self.commit = "a" * 40
        for name in material.NPM_FILES:
            path = self.root / "build/npm" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(("source:" + name).encode())
        for name in material.BINARY_NAMES:
            (self.dist / name).write_bytes(("binary:" + name).encode())
        self.pack()
        self.manifest()

    def pack(self, extra=None):
        with tarfile.open(self.dist / material.npm_name("0.1.0"), "w:gz") as archive:
            for name in material.NPM_FILES:
                data = (self.root / "build/npm" / name).read_bytes()
                info = tarfile.TarInfo("package/" + name)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
            if extra:
                archive.addfile(tarfile.TarInfo(extra), io.BytesIO())

    def manifest(self):
        rows = [{"name": name, "sha256": material.digest(self.dist / name), "bytes": (self.dist / name).stat().st_size}
                for name in material.asset_names("0.1.0")]
        self.data = {"version": "0.1.0", "repository": material.REPOSITORY, "commit": self.commit,
                     "published": False, "assets": rows}
        self.save_manifest()
        (self.dist / "checksums.txt").write_text("".join(f'{r["sha256"]}  {r["name"]}\n' for r in rows))

    def save_manifest(self):
        (self.dist / "release-manifest.json").write_text(json.dumps(self.data))

    def inspect(self, path):
        system, arch = path.name.removeprefix("wecom-channel-cli-").split("-")
        return {"GOOS": system, "GOARCH": arch, "vcs.revision": self.commit, "vcs.modified": "false"}

    def verify(self):
        return material.verify_assets(self.root, "0.1.0", self.commit, self.inspect)

    def test_complete_reviewed_candidate(self):
        self.assertEqual(self.verify(), [])

    def test_stale_commit(self):
        self.data["commit"] = "b" * 40
        self.save_manifest()
        self.assertTrue(self.verify())

    def test_incomplete_duplicate_and_traversing_manifests(self):
        original = list(self.data["assets"])
        for rows in ([], original[:-1], original[:-1] + [original[0]], [{**original[0], "name": "../outside"}] + original[1:]):
            with self.subTest(rows=rows):
                self.data["assets"] = rows
                self.save_manifest()
                self.assertTrue(self.verify())

    def test_malformed_manifest(self):
        (self.dist / "release-manifest.json").write_text("not json")
        self.assertTrue(self.verify())

    def test_wrong_platform_or_dirty_binary(self):
        for field, value in (("GOARCH", "wrong"), ("vcs.modified", "true"), ("vcs.revision", "b" * 40)):
            def inspect(path):
                return {**self.inspect(path), field: value}
            self.assertTrue(material.verify_assets(self.root, "0.1.0", self.commit, inspect))

    def test_binary_tampering(self):
        (self.dist / material.BINARY_NAMES[0]).write_bytes(b"changed")
        self.assertTrue(self.verify())

    def test_checksum_manifest_tampering(self):
        (self.dist / "checksums.txt").write_text("")
        self.assertTrue(self.verify())

    def test_source_changed_after_pack(self):
        (self.root / "build/npm/install.js").write_text("unreviewed installer")
        self.assertTrue(self.verify())

    def test_extra_npm_secret_file(self):
        self.pack(extra="package/.env")
        self.manifest()
        self.assertTrue(self.verify())

    def test_symlink_asset(self):
        path = self.dist / material.BINARY_NAMES[0]
        path.rename(self.dist / "moved")
        path.symlink_to(self.dist / "moved")
        self.assertTrue(self.verify())


class PublisherTest(unittest.TestCase):
    def test_first_publish_resolves_new_draft_before_verifying_assets(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            (root/'build/npm').mkdir(parents=True); (root/'dist').mkdir()
            (root/'build/npm/package.json').write_text(json.dumps({'name':'@angelmsger/wecom-channel-cli','version':'0.1.0'}))
            names=(*material.asset_names('0.1.0'),'checksums.txt','release-manifest.json')
            for name in names: (root/'dist'/name).write_bytes(name.encode())
            assets=[{'name':name,'state':'uploaded','digest':'sha256:'+material.digest(root/'dist'/name)} for name in names]
            draft={'id':123,'tag_name':'v0.1.0','draft':True,'assets':[]}
            calls=[]
            def require(argv):
                calls.append(argv)
                return 'a'*40 if argv[0]=='git' else ''
            def github(path, optional=False):
                if '/git/ref/' in path: return {'object':{'type':'commit','sha':'a'*40}}
                self.assertTrue(path.endswith('/releases/123'))
                return {**draft,'assets':assets}
            with mock.patch.object(publisher,'ROOT',root), mock.patch.object(publisher,'require',side_effect=require), \
                 mock.patch.object(publisher,'github',side_effect=github), \
                 mock.patch.object(publisher,'release_by_tag',side_effect=[None,draft]) as lookup, \
                 mock.patch.object(publisher,'npm_state',side_effect=['missing','missing','matching']), \
                 mock.patch.object(publisher.time,'sleep') as sleep, \
                 mock.patch('sys.argv',['publisher','--apply']), mock.patch.dict('os.environ',{},clear=True), \
                 mock.patch('sys.stdout',new_callable=io.StringIO):
                self.assertEqual(publisher.main(),0)
                self.assertEqual(lookup.call_count,2)
                sleep.assert_called_once_with(5)
            self.assertEqual(len([a for a in calls if a[:3]==['gh','release','create']]),1)
            self.assertEqual(len([a for a in calls if a[:3]==['gh','release','upload']]),7)
            self.assertEqual(len([a for a in calls if a[:2]==['npm','publish']]),1)

    def test_preview_does_not_publish_and_apply_never_clobbers(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "build/npm").mkdir(parents=True)
            (root / "dist").mkdir()
            (root / "build/npm/package.json").write_text(json.dumps({"name":"@angelmsger/wecom-channel-cli", "version":"0.1.0"}))
            names = (*material.asset_names("0.1.0"), "checksums.txt", "release-manifest.json")
            for name in names: (root / "dist" / name).write_bytes(name.encode())
            local = {name: material.digest(root / "dist" / name) for name in names}
            assets = [{"name": name, "state":"uploaded", "digest":"sha256:" + value} for name, value in local.items()]
            for apply in (False, True):
                calls = []
                def require(argv):
                    calls.append(argv)
                    return "a" * 40 if argv[0] == "git" else ""
                def github(path, optional=False):
                    if "/git/ref/" in path: return {"object":{"type":"commit", "sha":"a" * 40}}
                    self.assertTrue(path.endswith('/releases/123'))
                    return {"id":123,"tag_name":"v0.1.0","assets":assets,"draft":True}
                with mock.patch.object(publisher, "ROOT", root), mock.patch.object(publisher, "require", side_effect=require), \
                     mock.patch.object(publisher, "github", side_effect=github), mock.patch.object(publisher, "npm_state", return_value="matching"), \
                     mock.patch.object(publisher, "release_by_tag", return_value={"id":123,"tag_name":"v0.1.0","assets":assets[:1],"draft":True}), \
                     mock.patch("sys.argv", ["publisher", "--apply"] if apply else ["publisher"]), \
                     mock.patch.dict("os.environ", {}, clear=True), mock.patch("sys.stdout", new_callable=io.StringIO):
                    self.assertEqual(publisher.main(), 0)
                mutations = [a for a in calls if a[0] in ("gh", "npm")]
                if not apply:
                    self.assertEqual(mutations, [])
                else:
                    uploads = [a for a in mutations if a[1:3] == ["release","upload"]]
                    self.assertEqual(len(uploads), len(names)-1)
                    self.assertFalse(any("--clobber" in a for a in mutations))
                    self.assertTrue(any("--draft=false" in a for a in mutations))

    def test_draft_lookup_uses_database_identity(self):
        record = {"id":123,"tag_name":"v0.1.0","draft":True,"assets":[]}
        result = subprocess.CompletedProcess([],0,json.dumps({"databaseId":123,"tagName":"v0.1.0"}),"")
        with mock.patch.object(publisher,"run",return_value=result), mock.patch.object(publisher,"github",return_value=record) as read:
            self.assertEqual(publisher.release_by_tag("v0.1.0"),record)
            read.assert_called_once_with('repos/AngelMsger/wecom-channel-cli/releases/123')

    def test_release_lookup_distinguishes_absence_and_failure(self):
        for message, missing in (("release not found\n",True),("HTTP 403",False),("network unavailable",False)):
            with mock.patch.object(publisher,"run",return_value=subprocess.CompletedProcess([],1,"",message)):
                if missing:
                    self.assertIsNone(publisher.release_by_tag("v0.1.0"))
                else:
                    with self.assertRaises(RuntimeError): publisher.release_by_tag("v0.1.0")

    def test_release_identity_mismatch_stops(self):
        for identity in ({"databaseId":"123","tagName":"v0.1.0"},{"databaseId":123,"tagName":"wrong"}):
            with mock.patch.object(publisher,"run",return_value=subprocess.CompletedProcess([],0,json.dumps(identity),"")):
                with self.assertRaises(RuntimeError): publisher.release_by_tag("v0.1.0")
        with mock.patch.object(publisher,"run",return_value=subprocess.CompletedProcess([],0,'{"databaseId":123,"tagName":"v0.1.0"}',"")), \
             mock.patch.object(publisher,"github",return_value={"id":456,"tag_name":"v0.1.0"}):
            with self.assertRaises(RuntimeError): publisher.release_by_tag("v0.1.0")

    def test_missing_assets_only(self):
        self.assertEqual(publisher.upload_plan({"a": "aaa", "b": "bbb"},
                         [{"name": "a", "state": "uploaded", "digest": "sha256:aaa"}]), ["b"])

    def test_conflicts_and_unverified_assets_stop(self):
        for remote in ([{"name": "a", "state": "uploaded", "digest": "sha256:wrong"}],
                       [{"name": "a", "state": "uploaded"}], [{"name": "extra"}],
                       [{"name": "a", "state": "new", "digest": "sha256:aaa"}],
                       [{"name": "a", "state": "uploaded", "digest": "sha256:aaa"}] * 2):
            with self.assertRaises(RuntimeError):
                publisher.upload_plan({"a": "aaa"}, remote)

    def test_registry_missing_matching_and_conflict(self):
        for rc, value, expected in ((1, {"error": {"code": "E404"}}, "missing"),
                                   (0, "sha512-matching", "matching")):
            with mock.patch.object(publisher, "run", return_value=subprocess.CompletedProcess([], rc, json.dumps(value), "")):
                self.assertEqual(publisher.npm_state("test@1", "sha512-matching"), expected)
        for rc, output in ((1, '{"error":{"code":"E403"}}'), (1, "offline"), (0, '"sha512-different"')):
            with mock.patch.object(publisher, "run", return_value=subprocess.CompletedProcess([], rc, output, "")):
                with self.assertRaises(RuntimeError):
                    publisher.npm_state("test@1", "sha512-matching")

    def test_github_unknown_failure_is_not_absence(self):
        with mock.patch.object(publisher, "run", return_value=subprocess.CompletedProcess([], 1, "", "network unavailable")):
            with self.assertRaises(RuntimeError):
                publisher.github("repos/test/releases/tags/v1", optional=True)
        with mock.patch.object(publisher, "run", return_value=subprocess.CompletedProcess([], 1, "", "not found (HTTP 404)")):
            self.assertIsNone(publisher.github("repos/test/releases/tags/v1", optional=True))

    def test_registry_confirmation_is_bounded_without_republishing(self):
        with mock.patch.object(publisher, "npm_state", return_value="missing") as lookup, \
             mock.patch.object(publisher.time, "sleep") as sleep, \
             mock.patch.object(publisher, "require") as write:
            with self.assertRaisesRegex(RuntimeError, "test@1 is not confirmed"):
                publisher.confirm_npm("test@1", "sha512-matching")
            self.assertEqual(lookup.call_count, 7)
            self.assertEqual(sum(c.args[0] for c in sleep.call_args_list), 125)
            write.assert_not_called()

    def test_registry_confirmation_stops_on_unknown_or_conflicting_state(self):
        for rc, output in ((1, '{"error":{"code":"E403"}}'), (1, "offline"),
                           (0, '"sha512-different"'), (0, "not json")):
            with self.subTest(output=output), \
                 mock.patch.object(publisher, "run", return_value=subprocess.CompletedProcess([], rc, output, "")) as read, \
                 mock.patch.object(publisher.time, "sleep") as sleep:
                with self.assertRaises(RuntimeError):
                    publisher.confirm_npm("test@1", "sha512-matching")
                read.assert_called_once()
                self.assertIn("--prefer-online", read.call_args.args[0])
                self.assertIn("--fetch-retries=0", read.call_args.args[0])
                self.assertIn("--fetch-timeout=20000", read.call_args.args[0])
                sleep.assert_not_called()

    def test_npm_error_retains_only_code(self):
        result = subprocess.CompletedProcess([], 1, "private stdout", "npm error code E403\nnpm_token=secret\nhttps://example.test/auth?secret=value")
        with mock.patch.object(publisher, "run", return_value=result):
            with self.assertRaises(RuntimeError) as error:
                publisher.require(["npm", "publish", "archive.tgz"])
            self.assertIn("(E403)", str(error.exception))
            for private in ("private stdout", "secret", "example.test"):
                self.assertNotIn(private, str(error.exception))


if __name__ == "__main__":
    unittest.main()
